"""
Correr UNA sola vez localmente para generar token_gmail.json.

    python setup_gmail_auth.py

Requiere en GCP Console -> OAuth client -> Authorized redirect URIs:
    http://localhost:8080/
"""

import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from google_auth_oauthlib.flow import Flow

SCOPES   = ["https://www.googleapis.com/auth/gmail.readonly"]
REDIRECT = "http://localhost:8080"

flow = Flow.from_client_secrets_file(
    "credentials_gmail.json",
    scopes=SCOPES,
    redirect_uri=REDIRECT,
)

auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")
print(f"\nAbriendo browser para autenticacion...")
print(f"Si no abre automaticamente, copia este link:\n{auth_url}\n")
webbrowser.open(auth_url)

code_holder = []

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            code_holder.append(params["code"][0])
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Autenticacion OK. Podes cerrar esta ventana.")
    def log_message(self, *args):
        pass

print("Esperando respuesta de Google en http://localhost:8080/ ...")
server = HTTPServer(("localhost", 8080), Handler)
while not code_holder:
    server.handle_request()

flow.fetch_token(code=code_holder[0])
creds = flow.credentials

with open("token_gmail.json", "w") as f:
    f.write(creds.to_json())

print("token_gmail.json generado OK.")
