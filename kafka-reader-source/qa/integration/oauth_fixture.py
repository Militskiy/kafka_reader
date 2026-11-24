"""Optional loopback-only OAuth issuer and TLS Kafka listener for the disposable fixture."""
import base64
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import ssl
import subprocess
import threading
import time
from urllib.parse import parse_qs


def tls_credentials(directory, password):
    def openssl(*args):
        subprocess.run(['openssl', *args], cwd=directory, check=True, timeout=30,
                       env=dict(os.environ, FIXTURE_STORE_PASSWORD=password),
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-sha256',
            '-subj', '/CN=Kafka Reader ephemeral fixture CA', '-keyout', 'ca.key', '-out', 'ca.pem',
            '-addext', 'basicConstraints=critical,CA:TRUE,pathlen:0',
            '-addext', 'keyUsage=critical,keyCertSign,cRLSign')
    openssl('req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
            '-keyout', 'server.key', '-out', 'server.csr')
    (directory / 'server.ext').write_text('basicConstraints=critical,CA:FALSE\n'
        'keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n'
        'subjectAltName=IP:127.0.0.1,DNS:localhost\n', encoding='ascii')
    openssl('x509', '-req', '-in', 'server.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
            '-CAcreateserial', '-days', '1', '-sha256', '-extfile', 'server.ext', '-out', 'server.pem')
    openssl('pkcs12', '-export', '-out', 'server.p12', '-in', 'server.pem', '-inkey', 'server.key',
            '-certfile', 'ca.pem', '-passout', 'env:FIXTURE_STORE_PASSWORD')


def jwt_part(obj):
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip('=')


class Issuer:
    def __init__(self, session):
        self.session = session
        self.store_password = secrets.token_hex(16)
        self.scope = secrets.token_hex(32)
        self.current = secrets.token_urlsafe(32)
        self.username = 'fixture+reader-' + secrets.token_hex(8)
        self.password = 'fixture &=' + secrets.token_urlsafe(24)
        self.invalid_password = 'rejected+' + secrets.token_urlsafe(24)
        self.grants = {'refresh_token': 0, 'password': 0}
        self.requests = 0
        self.changed = threading.Condition()

    def exchange(self, form):
        with self.changed:
            self.requests += 1
            kind = form.get('grant_type')
            if kind == ['password']:
                expected = {'grant_type': ['password'], 'client_id': ['kafka-lifetime'],
                            'username': [self.username], 'password': [self.password]}
                if form != expected:
                    # Simulate an issuer echoing a rejected synthetic login in its response.
                    return 401, {'error': 'invalid_grant',
                                 'error_description': f'{self.username} {self.invalid_password}'}
            elif kind == ['refresh_token']:
                expected = {'grant_type': ['refresh_token'], 'client_id': ['kafka-cli'],
                            'refresh_token': [self.current]}
                if form != expected:
                    return 400, {'error': 'invalid_grant'}
                self.current = secrets.token_urlsafe(32)
            else:
                return 400, {'error': 'invalid_grant'}
            self.grants[kind[0]] += 1
            now = int(time.time())
            # Kafka's built-in unsecured validator is exclusively for this loopback test.
            claims = {'sub': 'reader', 'iat': now, 'exp': now + 90, 'scope': self.scope}
            access = jwt_part({'alg': 'none'}) + '.' + jwt_part(claims) + '.'
            self.changed.notify_all()
            response = {'token_type': 'Bearer', 'access_token': access, 'expires_in': 90}
            if kind == ['refresh_token']:
                response['refresh_token'] = self.current
            return 200, response

    def broker_config(self):
        prefix = 'listener.name.oauth.'
        return '\n'.join([
            prefix + 'sasl.enabled.mechanisms=OAUTHBEARER',
            prefix + 'ssl.keystore.type=PKCS12',
            prefix + 'ssl.keystore.location=server.p12',
            prefix + 'ssl.keystore.password=' + self.store_password,
            prefix + 'ssl.key.password=' + self.store_password,
            prefix + 'oauthbearer.sasl.jaas.config='
            'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
            f'unsecuredLoginStringClaim_sub="fixture-broker" unsecuredValidatorRequiredScope="{self.scope}";',
        ]) + '\n'

    def profile(self, broker_port, grant='refresh_token', *, invalid=False):
        path = self.session / f"oauth-{grant}{'-invalid' if invalid else ''}.properties"
        ca = self.session / 'ca.pem'
        if grant == 'password':
            password = self.invalid_password if invalid else self.password
            credentials = (f'oauth.grant.type=password\noauth.client.id=kafka-lifetime\n'
                           f'sasl.username={self.username}\nsasl.password={password}\n')
        elif grant == 'refresh_token':
            with self.changed:
                token = 'invalid-fixture-token' if invalid else self.current
            credentials = f'oauth.client.id=kafka-cli\noauth.refresh.token={token}\n'
        else:
            raise ValueError('Unknown fixture grant type')
        path.write_text(f'bootstrap.servers=127.0.0.1:{broker_port}\nsecurity.protocol=SASL_SSL\n'
            f'sasl.mechanism=OAUTHBEARER\nssl.ca.location={ca}\noauth.ssl.ca.location={ca}\n'
            f'oauth.token.endpoint.uri={self.endpoint}\n{credentials}oauth.max.token.expiry.seconds=4\n',
            encoding='utf-8')
        return path

    def wait_for_grants(self, grant, count):
        with self.changed:
            if not self.changed.wait_for(lambda: self.grants[grant] >= count, timeout=12):
                raise AssertionError(f'No OAuth renewal using {grant}')


@contextmanager
def start_issuer(session):
    issuer = Issuer(session)
    tls_credentials(session, issuer.store_password)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(session / 'server.pem', session / 'server.key')

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Keep request contents and grants out of retained logs.

        def do_POST(self):
            if self.path != '/token' or self.headers.get_content_type() != 'application/x-www-form-urlencoded':
                self.send_error(404)
                return
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 4096:
                    raise ValueError()
                form = parse_qs(self.rfile.read(size).decode('ascii'), strict_parsing=True)
            except ValueError:
                self.send_error(400)
                return
            status, payload = issuer.exchange(form)
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    class LocalTLS(ThreadingHTTPServer):
        def get_request(self):
            sock, address = super().get_request()
            sock.settimeout(3)
            return context.wrap_socket(sock, server_side=True), address

    with LocalTLS(('127.0.0.1', 0), Handler) as server:
        issuer.endpoint = f'https://127.0.0.1:{server.server_port}/token'
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield issuer
        finally:
            server.shutdown()
            thread.join()
