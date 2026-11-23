use crate::{
    model::Properties,
    oauth::{Grant, RefreshClient},
    remembered::RememberedCredential,
};
use crossbeam_channel::{TryRecvError, bounded};
use rdkafka::{
    ClientContext,
    client::{DefaultClientContext, OAuthToken},
    consumer::{BaseConsumer, Consumer, ConsumerContext},
    error::{KafkaError, RDKafkaErrorCode},
};
use std::{
    error::Error,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

/// In-memory credentials shared by a server's draft tests and subsequent reads.
/// A clone can switch accounts without invalidating the original server's session.
#[derive(Clone, Default)]
pub struct OAuthSession(Option<Arc<RefreshCredential>>);

struct RefreshCredential {
    endpoint: String,
    client_id: String,
    original: String,
    // Only workers take this lock across a network request.
    exchange_lock: Mutex<()>,
    state: Mutex<RefreshState>,
}

#[derive(Default)]
struct RefreshState {
    latest: Option<String>,
    remembered: Option<RememberedCredential>,
    /// Previous on-disk token if writing its replacement failed after a successful grant.
    pending_from: Option<String>,
}

impl RefreshCredential {
    fn exchange(
        &self,
        request: impl FnOnce(&str) -> Result<Grant, String>,
    ) -> Result<OAuthToken, String> {
        // A draft test and an active reader can share a single-use refresh token.
        let _exchange = self.exchange_lock.lock().unwrap();
        let (current, _saved_exchange) = {
            let mut state = self.state.lock().unwrap();
            // Another application instance can open the same remembered profile.
            // Keep this generation's grant lease until the successor is persisted.
            let lease = state
                .remembered
                .as_ref()
                .map(|s| s.lock_refresh())
                .transpose()?;
            if let Some(slot) = &state.remembered {
                if let (Some(previous), Some(latest)) = (&state.pending_from, &state.latest) {
                    slot.rotate(previous, latest)?;
                }
                // A different reader process may have renewed it since our last request.
                let token = slot
                    .load()?
                    .remove("oauth.refresh.token")
                    .ok_or("The remembered profile has no refresh token")?;
                state.latest = Some(token);
                state.pending_from = None;
            }
            (
                state.latest.as_ref().unwrap_or(&self.original).clone(),
                lease,
            )
        };
        let grant = request(&current)?;
        if let Some(next) = grant.refresh_token {
            let mut state = self.state.lock().unwrap();
            // The provider already spent the old token. Keep its successor in memory
            // even if DPAPI or the workspace write fails.
            let result = state
                .remembered
                .as_ref()
                .map(|slot| slot.rotate(&current, &next));
            state.latest = Some(next);
            if let Some(Err(error)) = result {
                state.pending_from = Some(current);
                return Err(format!("Could not save the rotated refresh token: {error}"));
            }
        }
        Ok(OAuthToken {
            token: grant.access_token,
            principal_name: grant.principal,
            lifetime_ms: grant.lifetime_ms,
        })
    }
}

impl OAuthSession {
    fn identity(props: &Properties) -> Option<(&str, &str, &str)> {
        let mechanism = props
            .get("sasl.mechanism")
            .or_else(|| props.get("sasl.mechanisms"));
        if !mechanism.is_some_and(|m| m.eq_ignore_ascii_case("OAUTHBEARER")) {
            return None;
        }
        let value = |k: &str| props.get(k).map(String::as_str).unwrap_or_default();
        Some((
            value("oauth.token.endpoint.uri"),
            value("oauth.client.id"),
            value("oauth.refresh.token"),
        ))
    }

    /// Can this handle reuse its possibly rotated credential for these settings?
    pub fn matches(&self, props: &Properties) -> bool {
        let Some(c) = self.0.as_ref() else {
            return false;
        };
        Self::identity(props)
            == Some((
                c.endpoint.as_str(),
                c.client_id.as_str(),
                c.original.as_str(),
            ))
    }

    /// Select the in-memory credential before saving or starting a request.
    /// This does not exchange a token or make a Kafka connection.
    pub fn bind(&mut self, props: &Properties) {
        if self.matches(props) {
            return;
        }
        let Some((endpoint, client_id, original)) = Self::identity(props) else {
            self.0 = None;
            return;
        };
        self.0 = Some(Arc::new(RefreshCredential {
            endpoint: endpoint.into(),
            client_id: client_id.into(),
            original: original.into(),
            exchange_lock: Mutex::default(),
            state: Mutex::default(),
        }));
    }

    /// Publish a new rotation target only after an in-flight grant has finished.
    /// Otherwise another process could spend its saved token before that grant returns.
    pub fn configure_new_remembered<T>(
        &self,
        save: impl FnOnce(&str) -> Result<(T, Option<RememberedCredential>), String>,
    ) -> Result<T, String> {
        let credential = self.0.as_ref().ok_or("OAuth refresh is not configured")?;
        let _exchange = credential
            .exchange_lock
            .try_lock()
            .map_err(|_| "Token refresh is in progress. Try saving again when it finishes.")?;
        self.configure_remembering(save)
    }

    /// Keep, restore or forget a rotation target. A grant in flight observes the
    /// updated target when it returns. The callback must not make network requests.
    pub fn configure_remembering<T>(
        &self,
        save: impl FnOnce(&str) -> Result<(T, Option<RememberedCredential>), String>,
    ) -> Result<T, String> {
        let credential = self.0.as_ref().ok_or("OAuth refresh is not configured")?;
        let mut state = credential.state.lock().unwrap();
        let (result, remembered) = save(state.latest.as_deref().unwrap_or(&credential.original))?;
        if state.remembered != remembered {
            // A newly saved slot already contains the snapshot; an unchanged slot
            // may still need to retry a previously failed compare-and-swap.
            state.pending_from = None;
        }
        state.remembered = remembered;
        Ok(result)
    }
}

pub(crate) struct ReaderContext {
    refresh: Option<(Arc<RefreshCredential>, RefreshClient)>,
    last_refresh: Mutex<Option<Result<(), String>>>,
}

impl ReaderContext {
    pub(crate) fn new(session: OAuthSession, properties: &Properties) -> Result<Self, String> {
        let refresh = if let Some(credential) = session.0 {
            if !properties
                .get("security.protocol")
                .is_some_and(|v| v.eq_ignore_ascii_case("SASL_SSL"))
            {
                return Err("Refresh-token authentication requires SASL_SSL".into());
            }
            if properties
                .get("enable.ssl.certificate.verification")
                .is_some_and(|v| !v.eq_ignore_ascii_case("true"))
                || properties
                    .get("ssl.endpoint.identification.algorithm")
                    .is_some_and(|v| !v.eq_ignore_ascii_case("https"))
            {
                return Err("Refresh-token authentication requires Kafka TLS certificate and hostname verification".into());
            }
            if credential.original.trim().is_empty() {
                return Err("Enter a refresh token for this session".into());
            }
            let max_seconds = properties
                .get("oauth.max.token.expiry.seconds")
                .map(|s| {
                    s.parse::<u64>().map_err(
                        |_| "OAuth maximum token lifetime must be a positive number of seconds",
                    )
                })
                .transpose()?
                .unwrap_or(300);
            let client = RefreshClient::new(
                &credential.endpoint,
                &credential.client_id,
                max_seconds,
                properties.get("oauth.ssl.ca.location").map(String::as_str),
            )?;
            Some((credential, client))
        } else {
            None
        };
        Ok(Self {
            refresh,
            last_refresh: Mutex::default(),
        })
    }

    pub(crate) fn uses_oauth(&self) -> bool {
        self.refresh.is_some()
    }

    pub(crate) fn check_refresh(&self) -> Result<(), String> {
        self.last_refresh.lock().unwrap().clone().unwrap_or(Ok(()))
    }

    /// The direct read queue carries only partition data. Serve the main queue even while
    /// metadata is blocking, the reader is paused, or its bounded record queue is full.
    pub(crate) fn with_token_refresh<T>(
        consumer: &BaseConsumer<Self>,
        read: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let context = consumer.context();
        if !context.uses_oauth() {
            return read();
        }
        // librdkafka enqueues an initial token request when the client is created.
        // Complete it before starting the first broker metadata timeout.
        let deadline = Instant::now() + Duration::from_secs(5);
        while context.last_refresh.lock().unwrap().is_none() {
            if Instant::now() >= deadline {
                return Err("Kafka client did not request an initial OAuth access token".into());
            }
            consumer.poll(Duration::from_millis(100));
        }
        context.check_refresh()?;
        thread::scope(|scope| {
            let (done, finished) = bounded::<()>(0);
            let poller = thread::Builder::new()
                .name("kafka-oauth-refresh".into())
                .spawn_scoped(scope, move || {
                    while finished.try_recv() == Err(TryRecvError::Empty) {
                        consumer.poll(Duration::from_millis(100));
                    }
                })
                .map_err(|_| "Could not start Kafka token refresh worker")?;
            let result = read();
            drop(done);
            poller
                .join()
                .map_err(|_| "Kafka token refresh worker stopped unexpectedly")?;
            context.check_refresh()?;
            result
        })
    }
}

impl ConsumerContext for ReaderContext {}

impl ClientContext for ReaderContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn error(&self, error: KafkaError, reason: &str) {
        if self.uses_oauth()
            && matches!(
                error,
                KafkaError::Global(
                    RDKafkaErrorCode::Authentication | RDKafkaErrorCode::SaslAuthenticationFailed
                )
            )
        {
            // Omit the broker's unstructured SASL reply. Keep a more specific
            // token-endpoint error if the grant itself has already failed.
            let mut status = self.last_refresh.lock().unwrap();
            if matches!(status.as_ref(), Some(Ok(()))) {
                *status = Some(Err("Kafka broker rejected OAuth authentication. Check the listener and token audience.".into()));
            }
        } else {
            DefaultClientContext.error(error, reason);
        }
    }

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        let (credential, client) = self
            .refresh
            .as_ref()
            .ok_or("OAuth refresh is not configured")?;
        let token = credential.exchange(|token| client.exchange(token));
        *self.last_refresh.lock().unwrap() = Some(token.as_ref().map(|_| ()).map_err(Clone::clone));
        Ok(token?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> Properties {
        [
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanisms", "OAUTHBEARER"),
            ("oauth.token.endpoint.uri", "https://id.example/token"),
            ("oauth.client.id", "kafka-cli"),
            ("oauth.refresh.token", "initial"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }

    fn grant(refresh_token: Option<&str>) -> Result<Grant, String> {
        Ok(Grant {
            access_token: "access".into(),
            principal: "test".into(),
            lifetime_ms: 1_000_000,
            refresh_token: refresh_token.map(str::to_owned),
        })
    }

    #[test]
    fn rotation_survives_test_save_read_and_canceled_draft_edits() {
        let mut props = profile();
        let mut draft = OAuthSession::default();
        draft.bind(&props);
        draft
            .0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "initial");
                grant(Some("rotated"))
            })
            .unwrap();
        let mut saved = draft.clone();
        saved.bind(&props);
        saved
            .0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "rotated");
                grant(None)
            })
            .unwrap();
        // No rotation in this response; the next request must keep using "rotated".
        let mut edit = saved.clone();
        props.insert("oauth.refresh.token".into(), "new credential".into());
        edit.bind(&props);
        edit.0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "new credential");
                grant(Some("new rotation"))
            })
            .unwrap();
        saved
            .0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "rotated");
                grant(None)
            })
            .unwrap();
    }

    #[test]
    fn testing_an_untested_saved_profile_preserves_rotation_on_cancel() {
        let props = profile();
        let mut saved = OAuthSession::default();
        saved.bind(&props);
        let mut edit = saved.clone();
        edit.bind(&props);
        edit.0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "initial");
                grant(Some("new refresh"))
            })
            .unwrap();
        drop(edit);
        saved.bind(&props);
        saved
            .0
            .as_ref()
            .unwrap()
            .exchange(|token| {
                assert_eq!(token, "new refresh");
                grant(None)
            })
            .unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn remember_the_latest_grant_and_forget_during_renewal() {
        use crate::{
            remembered::RememberChange,
            workspace::{Server, Store, Workspace},
        };
        let store = Store::for_gui(None, true).unwrap();
        let mut workspace = store.load().unwrap();
        let mut props = profile();
        props.insert("bootstrap.servers".into(), "qa.invalid:9093".into());
        let mut public = props.clone();
        public.remove("oauth.refresh.token");
        workspace.servers.push(Server {
            id: 1,
            name: "QA".into(),
            offline: false,
            properties: public,
            topics: vec![],
        });
        workspace.selected_server = 1;
        let mut session = OAuthSession::default();
        session.bind(&props);
        let credential = session.0.as_ref().unwrap();
        // Testing the draft consumed the initial token before remember was selected.
        credential
            .exchange(|_| {
                assert!(
                    session
                        .configure_new_remembered::<()>(|_| panic!("published an in-flight token"))
                        .is_err()
                );
                grant(Some("after-test"))
            })
            .unwrap();
        let slot = session
            .configure_new_remembered(|token| {
                assert_eq!(token, "after-test");
                let secrets = Properties::from([("oauth.refresh.token".into(), token.into())]);
                store.save_with_credential(&mut workspace, 1, RememberChange::Replace(&secrets))?;
                let slot = store.remembered_credential(&workspace, 1).unwrap();
                Ok((slot.clone(), Some(slot)))
            })
            .unwrap();
        credential
            .exchange(|token| {
                assert_eq!(token, "after-test");
                grant(Some("after-read"))
            })
            .unwrap();
        assert_eq!(slot.load().unwrap()["oauth.refresh.token"], "after-read");

        // A new process can spend a rotation written by the receiver of the grant.
        let reopened_store = Store::new(Some(store.directory.clone()));
        let reopened: Workspace = reopened_store.load().unwrap();
        let reopened_slot = reopened_store.remembered_credential(&reopened, 1).unwrap();
        reopened_slot
            .rotate("after-read", "from-another-instance")
            .unwrap();
        let (started, request_started) = bounded(1);
        let (release, send_grant) = bounded(1);
        thread::scope(|scope| {
            let task = scope.spawn(move || {
                credential.exchange(|token| {
                    assert_eq!(token, "from-another-instance");
                    started.send(()).unwrap();
                    send_grant.recv_timeout(Duration::from_secs(5)).unwrap();
                    grant(Some("session-only-again"))
                })
            });
            request_started
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            // The grant currently in flight owns the shared on-disk generation.
            let mut peer = OAuthSession::default();
            peer.bind(&props);
            peer.configure_remembering(|_| Ok(((), Some(reopened_slot.clone()))))
                .unwrap();
            assert!(
                peer.0
                    .as_ref()
                    .unwrap()
                    .exchange(|_| { panic!("concurrent grant spent a remembered token twice") })
                    .is_err()
            );
            session
                .configure_remembering(|token| {
                    assert_eq!(token, "from-another-instance");
                    store.save_with_credential(&mut workspace, 1, RememberChange::Forget)?;
                    Ok(((), None))
                })
                .unwrap();
            release.send(()).unwrap();
            task.join().unwrap().unwrap();
        });
        assert!(slot.load().is_err());
        assert!(reopened_store.load().unwrap().remembered.is_empty());
        assert!(
            store
                .directory
                .read_dir()
                .unwrap()
                .flatten()
                .all(|e| { e.path().extension().is_none_or(|ext| ext != "dpapi") })
        );
        credential
            .exchange(|token| {
                assert_eq!(token, "session-only-again");
                grant(None)
            })
            .unwrap();
    }

    #[test]
    fn oauth_requires_authenticated_tls_on_the_kafka_connection() {
        for (key, value) in [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("security.protocol", "SSL"),
            ("enable.ssl.certificate.verification", "false"),
            ("ssl.endpoint.identification.algorithm", "none"),
        ] {
            let mut props = profile();
            props.insert(key.into(), value.into());
            let mut session = OAuthSession::default();
            session.bind(&props);
            assert!(ReaderContext::new(session, &props).is_err(), "{key}");
        }
    }
}
