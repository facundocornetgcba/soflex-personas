"""
Gmail utilities: busca el adjunto mas reciente del exportable BAP Personas.
"""

import os
import base64
import json

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

SENDER  = "no.responder108.soflex@gmail.com"
SUBJECT = "Informe BAP Personas"


def _get_gmail_service():
    token_path = os.getenv("GMAIL_TOKEN_PATH", "token_gmail.json")
    creds_path = os.getenv("GMAIL_CREDENTIALS_PATH", "credentials_gmail.json")

    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "w") as f:
                f.write(creds.to_json())
        else:
            raise RuntimeError(
                "token_gmail.json no existe o es invalido. "
                "Corre setup_gmail_auth.py localmente primero."
            )

    return build("gmail", "v1", credentials=creds)


def get_latest_excel_from_gmail() -> bytes:
    """
    Busca el mail mas reciente de SENDER con SUBJECT en el asunto
    y devuelve el adjunto Excel como bytes.
    Lanza RuntimeError si no encuentra nada.
    """
    service = _get_gmail_service()

    query = f"from:{SENDER} subject:{SUBJECT}"
    result = service.users().messages().list(
        userId="me", q=query, maxResults=5
    ).execute()

    messages = result.get("messages", [])
    if not messages:
        raise RuntimeError(
            f"No se encontro ningun mail de {SENDER} con asunto '{SUBJECT}'."
        )

    # El primero es el mas reciente (Gmail devuelve en orden descendente)
    msg_id = messages[0]["id"]
    msg = service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute()

    parts = msg.get("payload", {}).get("parts", [])
    for part in parts:
        filename = part.get("filename", "")
        mime = part.get("mimeType", "")
        if filename and (
            filename.endswith(".xls")
            or filename.endswith(".xlsx")
            or "spreadsheet" in mime
            or "excel" in mime
            or "ms-excel" in mime
        ):
            body = part.get("body", {})
            att_id = body.get("attachmentId")
            if att_id:
                att = service.users().messages().attachments().get(
                    userId="me", messageId=msg_id, id=att_id
                ).execute()
                data = att.get("data", "")
            else:
                data = body.get("data", "")

            return base64.urlsafe_b64decode(data + "==")

    raise RuntimeError(
        f"Mail encontrado (id={msg_id}) pero no tiene adjunto Excel."
    )
