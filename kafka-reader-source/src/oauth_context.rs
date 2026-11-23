use crate::{
    model::Properties,
    oauth::{Grant, RefreshClient},
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
    rotated: Mutex<Option<String>>,
}

impl RefreshCredential {
    fn exchange(
        &self,
        request: impl FnOnce(&str) -> Result<Grant, String>,
    ) -> Result<OAuthToken, String> {
        // A draft test and an active reader can share a rotating token. Only one
        // grant may spend that token at a time. No UI operation takes this lock.
        let mut rotated = self.rotated.lock().unwrap();
        let grant = request(rotated.as_deref().unwrap_or(&self.original))?;
        if let Some(next) = grant.refresh_token {
            *rotated = Some(next);
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
            rotated: Mutex::default(),
        }));
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
