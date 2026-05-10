#!/usr/bin/env python3
"""
Dispara un evento Event Grid (BlobCreated) contra una Azure Function local.

Uso basico:
  python scripts/send_eventgrid_blobcreated.py \
    --blob-url "https://<account>.blob.core.windows.net/images/<blob-path>"

Ejemplo con ruta de endpoint personalizada:
  python scripts/send_eventgrid_blobcreated.py \
    --blob-url "https://.../images/usuario@mail.com~20260427T150000Z~uuid.jpg" \
    --endpoint "http://localhost:7071/runtime/webhooks/EventGrid?functionName=visorapp"
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = "http://localhost:7071/runtime/webhooks/EventGrid?functionName=visorapp"


def _parse_blob(blob_url: str) -> tuple[str, str]:
    parsed = urlparse(blob_url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError(
            "La URL del blob debe contener al menos /<container>/<blob-path>."
        )
    container = unquote(parts[0])
    blob_path = unquote("/".join(parts[1:]))
    return container, blob_path


def _build_subject(container: str, blob_path: str) -> str:
    return (
        f"/blobServices/default/containers/{container}/blobs/{blob_path}"
    )


def _build_event(blob_url: str, container: str, blob_path: str) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    event = {
        "id": str(uuid.uuid4()),
        "topic": "/subscriptions/local/resourceGroups/local/providers/Microsoft.Storage/storageAccounts/localdevstore",
        "subject": _build_subject(container, blob_path),
        "data": {
            "api": "PutBlob",
            "clientRequestId": str(uuid.uuid4()),
            "requestId": str(uuid.uuid4()),
            "eTag": "0x8DLOCALTEST",
            "contentType": "image/jpeg",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": blob_url,
            "sequencer": "0000000000000000000000000000000000000000000000000000000000000000",
            "storageDiagnostics": {"batchId": str(uuid.uuid4())},
        },
        "eventType": "Microsoft.Storage.BlobCreated",
        "eventTime": now,
        "metadataVersion": "1",
        "dataVersion": "1",
    }
    return [event]


def send_event(endpoint: str, payload: list[dict], timeout: int) -> tuple[int, str]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "aeg-event-type": "Notification",
        },
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        response_body = response.read().decode("utf-8", errors="replace")
        return response.status, response_body


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Emula un evento Event Grid BlobCreated para la Function local."
    )
    parser.add_argument(
        "--blob-url",
        required=True,
        help="URL completa del blob ya existente en Storage.",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"Endpoint Event Grid local (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    args = parser.parse_args()

    try:
        container, blob_path = _parse_blob(args.blob_url)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    payload = _build_event(args.blob_url, container, blob_path)

    print("[INFO] Enviando evento Event Grid...")
    print(f"[INFO] Endpoint: {args.endpoint}")
    print(f"[INFO] Blob URL: {args.blob_url}")
    print(f"[INFO] Container: {container}")
    print(f"[INFO] Blob path: {blob_path}")

    try:
        status, body = send_event(args.endpoint, payload, args.timeout)
    except Exception as exc:
        print(f"[ERROR] Fallo al enviar evento: {exc}", file=sys.stderr)
        return 1

    print(f"[OK] HTTP {status}")
    if body.strip():
        print("[RESPUESTA]")
        print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
