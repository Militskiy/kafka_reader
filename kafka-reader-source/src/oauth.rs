//! OAuth refresh-token grant for a public Kafka client.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use std::{
    fs::File,
    io::Read,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use ureq::{
    Agent,
    config::ConfigBuilder,
    http::Uri,
    tls::{PemItem, RootCerts, TlsConfig, TlsProvider, parse_pem},
    typestate::AgentScope,
};

const MAX_RESPONSE: u64 = 128 * 1024;
const MAX_CA_BYTES: u64 = 8 * 1024 * 1024;

pub fn validate_endpoint(value: &str) -> Result<(), String> {
    let error = "Enter an HTTPS OAuth token endpoint without embedded credentials or a fragment";
    let uri = value.parse::<Uri>().map_err(|_| error.to_string())?;
    if uri.scheme_str() != Some("https")
        || uri.host().is_none_or(str::is_empty)
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
        || value.contains('#')
        || value.chars().any(char::is_whitespace)
    {
        return Err(error.into());
    }
    Ok(())
}

pub(crate) struct RefreshClient {
    agent: Agent,
    endpoint: String,
    client_id: String,
    max_lifetime_seconds: u64,
}

pub(crate) struct Grant {
    pub access_token: String,
    pub principal: String,
    pub lifetime_ms: i64,
    pub refresh_token: Option<String>,
}

impl RefreshClient {
    pub(crate) fn new(
        endpoint: &str,
        client_id: &str,
        max_lifetime_seconds: u64,
        ca_path: Option<&str>,
    ) -> Result<Self, String> {
        validate_endpoint(endpoint)?;
        if client_id.trim().is_empty() || client_id.chars().any(char::is_control) {
            return Err("Enter an OAuth client ID".into());
        }
        if max_lifetime_seconds == 0 {
            return Err("OAuth maximum token lifetime must be positive".into());
        }
        let roots = match ca_path.filter(|path| !path.is_empty()) {
            Some(path) => pem_roots(path)?,
            None => RootCerts::PlatformVerifier,
        };
        Ok(Self {
            agent: token_agent(Agent::config_builder(), roots),
            endpoint: endpoint.into(),
            client_id: client_id.into(),
            max_lifetime_seconds,
        })
    }

    pub(crate) fn exchange(&self, refresh_token: &str) -> Result<Grant, String> {
        if refresh_token.trim().is_empty() || refresh_token.chars().any(char::is_control) {
            return Err("Enter an OAuth refresh token for this session".into());
        }
        let started_ms = unix_millis()?;
        let mut response = self
            .agent
            .post(self.endpoint.as_str())
            .header("Accept", "application/json")
            .send_form([
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("refresh_token", refresh_token),
            ])
            .map_err(|error| match error {
                ureq::Error::Timeout(_) => "OAuth token endpoint timed out",
                ureq::Error::NativeTls(_) | ureq::Error::Tls(_) => {
                    "OAuth token endpoint TLS verification or handshake failed"
                }
                _ => "Could not connect to the OAuth token endpoint",
            })?;
        let status = response.status();
        if !status.is_success() {
            // An identity provider's response body can echo the submitted credentials.
            return Err(format!(
                "OAuth token endpoint returned HTTP {}",
                status.as_u16()
            ));
        }
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE)
            .read_to_vec()
            .map_err(|_| "Could not read OAuth token response within the size or time limit")?;
        let response: TokenResponse =
            serde_json::from_slice(&body).map_err(|_| "Invalid OAuth token response")?;
        response.into_grant(
            &self.client_id,
            self.max_lifetime_seconds,
            started_ms,
            unix_millis()?,
        )
    }
}

fn token_agent(config: ConfigBuilder<AgentScope>, roots: RootCerts) -> Agent {
    config
        .timeout_global(Some(Duration::from_secs(10)))
        .max_redirects(0)
        .http_status_as_error(false)
        .tls_config(
            TlsConfig::builder()
                .provider(TlsProvider::NativeTls)
                .root_certs(roots)
                .build(),
        )
        .build()
        .new_agent()
}

fn pem_roots(path: &str) -> Result<RootCerts, String> {
    let mut pem = Vec::new();
    File::open(path)
        .and_then(|f| f.take(MAX_CA_BYTES + 1).read_to_end(&mut pem))
        .map_err(|_| "Could not read the OAuth CA certificate file")?;
    if pem.len() as u64 > MAX_CA_BYTES {
        return Err("OAuth CA certificate file exceeds 8 MiB".into());
    }
    let mut certs = Vec::new();
    for item in parse_pem(&pem) {
        if let PemItem::Certificate(cert) = item.map_err(|_| "Invalid OAuth CA certificate file")? {
            certs.push(cert);
        }
    }
    if certs.is_empty() {
        return Err("OAuth CA file contains no PEM certificates".into());
    }
    Ok(RootCerts::Specific(certs.into()))
}

fn unix_millis() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "System clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).map_err(|_| "System clock is out of range".into())
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: Option<String>,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
}

#[derive(Default, Deserialize)]
struct Claims {
    exp: Option<i64>,
    sub: Option<String>,
}

impl TokenResponse {
    fn into_grant(
        self,
        client_id: &str,
        max_seconds: u64,
        started_ms: i64,
        now_ms: i64,
    ) -> Result<Grant, String> {
        if self.access_token.is_empty()
            || !self.access_token.bytes().all(|b| b.is_ascii_graphic())
            || self
                .token_type
                .as_ref()
                .is_some_and(|s| !s.eq_ignore_ascii_case("bearer"))
        {
            return Err("OAuth token response did not contain a usable bearer access token".into());
        }
        // JWT claims can shorten the local refresh schedule. The broker authenticates the token.
        let claims = self
            .access_token
            .split('.')
            .nth(1)
            .and_then(|p| URL_SAFE_NO_PAD.decode(p).ok())
            .and_then(|p| serde_json::from_slice::<Claims>(&p).ok())
            .unwrap_or_default();
        let seconds = self.expires_in.unwrap_or(max_seconds).min(max_seconds);
        let duration_ms = i64::try_from(u128::from(seconds) * 1000)
            .map_err(|_| "OAuth token lifetime is out of range")?;
        let mut lifetime_ms = started_ms
            .checked_add(duration_ms)
            .ok_or("OAuth token lifetime is out of range")?;
        if let Some(exp) = claims.exp {
            lifetime_ms = lifetime_ms.min(exp.saturating_mul(1000));
        }
        // librdkafka converts the absolute expiry to signed microseconds.
        if lifetime_ms > i64::MAX / 1000 {
            return Err("OAuth token lifetime is out of range".into());
        }
        if lifetime_ms <= now_ms {
            return Err("OAuth token endpoint returned an expired access token".into());
        }
        let principal = claims
            .sub
            .filter(|s| !s.trim().is_empty() && !s.chars().any(char::is_control))
            .unwrap_or_else(|| client_id.to_owned());
        if self
            .refresh_token
            .as_deref()
            .is_some_and(|s| s.trim().is_empty() || s.chars().any(char::is_control))
        {
            return Err(
                "OAuth token endpoint returned an invalid replacement refresh token".into(),
            );
        }
        Ok(Grant {
            access_token: self.access_token,
            principal,
            lifetime_ms,
            refresh_token: self.refresh_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        thread,
    };

    #[test]
    fn only_https_endpoints_without_credentials_or_fragments_are_accepted() {
        for endpoint in [
            "http://127.0.0.1/token",
            "https://user:secret@sso.invalid/token",
            "https://sso.invalid/token#fragment",
            "sso.invalid/token",
            "https://",
            "https://sso.invalid/token path",
        ] {
            assert!(validate_endpoint(endpoint).is_err());
        }
        assert!(validate_endpoint("https://sso.invalid:8443/realm/token?realm=one").is_ok());
    }

    #[test]
    fn expiry_uses_the_earliest_of_response_jwt_and_client_limit() {
        let jwt = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(br#"{"sub":"fixture","exp":120}"#)
        );
        for (expires_in, cap, expected_ms) in
            [(30, 300, 120_000), (5, 300, 105_000), (30, 2, 102_000)]
        {
            let response = TokenResponse {
                access_token: jwt.clone(),
                token_type: Some("bearer".into()),
                expires_in: Some(expires_in),
                refresh_token: None,
            };
            let grant = response
                .into_grant("client", cap, 100_000, 100_500)
                .unwrap();
            assert_eq!(grant.lifetime_ms, expected_ms);
            assert_eq!(grant.principal, "fixture");
        }
    }

    #[test]
    fn expired_or_malformed_tokens_do_not_reach_the_broker() {
        for json in [
            r#"{"access_token":"opaque","expires_in":0}"#,
            r#"{"access_token":"opaque","expires_in":1}"#,
            r#"{"access_token":"opaque","token_type":"MAC"}"#,
            r#"{"access_token":"opaque\u0000","expires_in":30}"#,
            r#"{"access_token":"opaque","refresh_token":""}"#,
        ] {
            let response: TokenResponse = serde_json::from_str(json).unwrap();
            assert!(
                response
                    .into_grant("client", 300, 100_000, 101_000)
                    .is_err()
            );
        }
    }

    // Production only accepts HTTPS. Exercise the same request and response handling
    // with a loopback socket and an explicit no-proxy transport for deterministic tests.
    fn mock_exchange(status: &str, body: &str) -> Result<Grant, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        thread::scope(|scope| {
            let server = scope.spawn(|| {
                let (mut stream, _) = listener.accept().unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let mut reader = BufReader::new(&stream);
                let mut headers = String::new();
                while !headers.ends_with("\r\n\r\n") {
                    assert!(reader.read_line(&mut headers).unwrap() > 0);
                }
                assert!(headers.starts_with("POST /token HTTP/1.1\r\n"));
                let headers = headers.to_ascii_lowercase();
                assert!(headers.contains("content-type: application/x-www-form-urlencoded"));
                let length = headers.lines().find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap().parse::<usize>().unwrap();
                let mut form = vec![0; length];
                reader.read_exact(&mut form).unwrap();
                assert_eq!(form, b"grant_type=refresh_token&client_id=kafka%2Bcli&refresh_token=sample%2B%26%2F%3D");
                // A 307 would replay the secret if redirects were followed.
                let head = format!("HTTP/1.1 {status}\r\nLocation: http://{addr}/unexpected\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
                stream.write_all(head.as_bytes()).unwrap();
                // The client may close after the headers on status or size errors.
                let _ = stream.write_all(body.as_bytes());
            });
            let client = RefreshClient {
                agent: token_agent(
                    Agent::config_builder().proxy(None),
                    RootCerts::PlatformVerifier,
                ),
                endpoint: format!("http://{addr}/token"),
                client_id: "kafka+cli".into(),
                max_lifetime_seconds: 300,
            };
            let result = client.exchange("sample+&/=");
            server.join().unwrap();
            result
        })
    }

    #[test]
    fn exchanges_encoded_form_and_returns_rotated_token() {
        let grant = mock_exchange("200 OK", r#"{"access_token":"opaque","token_type":"Bearer","expires_in":600,"refresh_token":"replacement"}"#).unwrap();
        assert_eq!(grant.access_token, "opaque");
        assert_eq!(grant.principal, "kafka+cli");
        assert_eq!(grant.refresh_token.as_deref(), Some("replacement"));
        assert!(grant.lifetime_ms <= unix_millis().unwrap() + 300_000);
    }

    #[test]
    fn no_redirect_replay_no_echoed_secrets_and_bounded_response() {
        for (status, code) in [("400 Bad Request", 400), ("307 Temporary Redirect", 307)] {
            let error = mock_exchange(status, "fixture-credential-echo")
                .err()
                .unwrap();
            assert_eq!(error, format!("OAuth token endpoint returned HTTP {code}"));
        }
        assert!(mock_exchange("200 OK", &"x".repeat(MAX_RESPONSE as usize + 1)).is_err());
    }
}
