import datetime
import hashlib
import io
import logging
import os
import re
import unicodedata
import uuid
from urllib.parse import unquote, urlparse

import azure.functions as func
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import (
    CosmosHttpResponseError,
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Contenedor donde Event Grid entrega solo creaciones de blobs (capturas).
IMAGES_CONTAINER = os.getenv("VISORAPP_IMAGES_CONTAINER", "images").strip().lower()

# Idempotencia / deduplicación: contenedor Cosmos aparte (partition key path: /pk).
# Crear en la misma cuenta que player-stats, p.ej. DB "pogo-db", PK /pk.
COSMOS_LOCKS_CONTAINER = os.getenv("COSMOS_LOCKS_CONTAINER", "image-processing-locks").strip()
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE", "pogo-db").strip()
# Si el lock está in_progress y el arranque fue hace menos de estos segundos, se trata como
# el mismo trabajo (duplicados de Event Grid o procesamiento en curso). Pasado ese tiempo,
# otro intento puede tomar el lock (worker colgado o reintento legítimo).
PROCESSING_STALE_AFTER_SECONDS = int(os.getenv("VISORAPP_PROCESSING_STALE_AFTER_SECONDS", "900"))
_LOCKS_DISABLED = os.getenv("VISORAPP_DISABLE_PROCESSING_LOCKS", "").lower() in (
    "1",
    "true",
    "yes",
)


def _event_data_dict(event: func.EventGridEvent | func.CloudEvent) -> dict:
    raw = event.get_json()
    return raw if isinstance(raw, dict) else {}


def _event_type_name(event: func.EventGridEvent | func.CloudEvent) -> str | None:
    if isinstance(event, func.CloudEvent):
        return event.type
    return getattr(event, "event_type", None)


def parse_blob_created_from_event(
    event: func.EventGridEvent | func.CloudEvent,
) -> dict | None:
    """
    Extrae URL, contenedor y ruta del blob desde Microsoft.Storage.BlobCreated
    (Event Grid schema o CloudEvents).
    """
    evt_type = (_event_type_name(event) or "").strip()
    if evt_type.lower() != "microsoft.storage.blobcreated":
        logging.info("Evento ignorado (tipo no BlobCreated): %s", evt_type)
        return None

    data = _event_data_dict(event)
    url = (data.get("url") or "").strip()
    subject = (getattr(event, "subject", None) or "").strip()

    container: str | None = None
    blob_path: str | None = None

    if url:
        try:
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 2:
                container = unquote(parts[0])
                blob_path = unquote("/".join(parts[1:]))
        except Exception as exc:
            logging.warning("Error parseando URL del blob %r: %s", url, exc)

    if (not container or not blob_path) and subject:
        m = re.search(
            r"/blobServices/default/containers/([^/]+)/blobs/(.+)$",
            subject,
            re.IGNORECASE,
        )
        if m:
            container = unquote(m.group(1))
            blob_path = unquote(m.group(2))

    if not container or not blob_path:
        logging.warning(
            "No se pudo deducir contenedor/blob. url=%r subject=%r",
            url,
            subject,
        )
        return None

    content_length = data.get("contentLength")
    if content_length is not None:
        try:
            content_length = int(content_length)
        except (TypeError, ValueError):
            content_length = None

    return {
        "url": url or None,
        "container": container,
        "blob_path": blob_path,
        "content_length": content_length,
    }


def _normalized_blob_url_for_lock(blob_url: str) -> str:
    parsed = urlparse(blob_url.strip())
    path = parsed.path or ""
    return f"{(parsed.scheme or 'https').lower()}://{(parsed.netloc or '').lower()}{path}"


def _lock_id_and_pk(blob_url: str) -> tuple[str, str]:
    normalized = _normalized_blob_url_for_lock(blob_url)
    lock_id = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    host = (urlparse(blob_url).hostname or "unknown-storage").lower()
    return lock_id, host


def _parse_iso_utc(value: str | None) -> datetime.datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except ValueError:
        return None


def _locks_container_client(cosmos: CosmosClient):
    if _LOCKS_DISABLED or not COSMOS_LOCKS_CONTAINER:
        return None
    try:
        return cosmos.get_database_client(COSMOS_DATABASE).get_container_client(
            COSMOS_LOCKS_CONTAINER
        )
    except Exception as exc:
        logging.warning("No se pudo abrir contenedor de locks: %s", exc)
        return None


def try_begin_processing(
    locks,
    *,
    lock_id: str,
    partition_key: str,
    blob_url: str,
    container: str,
    blob_path: str,
    event_grid_id: str | None,
) -> bool:
    """
    True si esta invocación debe procesar el blob; False si es duplicado o competidor activo.
    """
    if locks is None:
        return True

    now = datetime.datetime.now(datetime.timezone.utc)
    now_iso = now.isoformat()

    def _age_seconds(doc: dict) -> float | None:
        started = _parse_iso_utc(doc.get("startedAt"))
        if started is None:
            return None
        return (now - started).total_seconds()

    initial_body = {
        "id": lock_id,
        "pk": partition_key,
        "status": "in_progress",
        "startedAt": now_iso,
        "blobUrl": blob_url,
        "container": container,
        "blobPath": blob_path,
        "lastEventGridId": event_grid_id,
    }

    try:
        locks.create_item(body=initial_body)
        logging.info("Lock adquirido (create) para blob pk=%s id=%s", partition_key, lock_id)
        return True
    except CosmosResourceExistsError:
        pass
    except CosmosHttpResponseError as exc:
        if getattr(exc, "status_code", None) == 409:
            pass
        else:
            logging.warning(
                "Lock create inesperado (se continúa sin deduplicación fuerte): %s",
                exc,
            )
            return True
    except CosmosResourceNotFoundError:
        logging.warning(
            "Contenedor de locks %r no existe en %r; procesando sin deduplicación.",
            COSMOS_LOCKS_CONTAINER,
            COSMOS_DATABASE,
        )
        return True
    except Exception as exc:
        logging.warning("Lock create falló; se procesa igualmente: %s", exc)
        return True

    for attempt in range(5):
        try:
            doc = dict(locks.read_item(item=lock_id, partition_key=partition_key))
        except CosmosResourceNotFoundError:
            try:
                locks.create_item(body=initial_body)
                return True
            except CosmosResourceExistsError:
                continue
        except Exception as exc:
            logging.warning("Lock read falló; se procesa: %s", exc)
            return True

        status = (doc.get("status") or "").lower()
        if status == "completed":
            logging.info(
                "Blob ya procesado (lock completed). Se omite. id=%s",
                lock_id,
            )
            return False

        age = _age_seconds(doc)
        if status == "in_progress" and age is not None and age < PROCESSING_STALE_AFTER_SECONDS:
            logging.info(
                "Lock in_progress desde hace %.0fs (< %ss); se omite (duplicado o job activo). id=%s",
                age,
                PROCESSING_STALE_AFTER_SECONDS,
                lock_id,
            )
            return False

        doc["status"] = "in_progress"
        doc["startedAt"] = now_iso
        doc["lastEventGridId"] = event_grid_id
        doc["blobUrl"] = blob_url
        doc["container"] = container
        doc["blobPath"] = blob_path
        try:
            locks.replace_item(item=lock_id, body=doc)
            logging.info("Lock tomado (replace stale) id=%s intento=%s", lock_id, attempt)
            return True
        except CosmosHttpResponseError as exc:
            if getattr(exc, "status_code", None) == 412 and attempt < 4:
                logging.info("Lock replace 412; reintentando id=%s", lock_id)
                continue
            logging.warning("Lock replace falló; se procesa: %s", exc)
            return True
        except Exception as exc:
            logging.warning("Lock replace falló; se procesa: %s", exc)
            return True

    logging.warning(
        "Agotados reintentos de lock por contención; se procesa para no perder el evento."
    )
    return True


def mark_processing_completed(locks, *, lock_id: str, partition_key: str) -> None:
    if locks is None:
        return
    try:
        doc = dict(locks.read_item(item=lock_id, partition_key=partition_key))
        doc["status"] = "completed"
        doc["completedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        locks.replace_item(item=lock_id, body=doc)
    except Exception as exc:
        logging.warning("No se pudo marcar lock completed id=%s: %s", lock_id, exc)


def mark_processing_failed(locks, *, lock_id: str, partition_key: str, message: str) -> None:
    if locks is None:
        return
    try:
        doc = dict(locks.read_item(item=lock_id, partition_key=partition_key))
        doc["status"] = "failed"
        doc["failedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        doc["lastError"] = (message or "")[:1024]
        locks.replace_item(item=lock_id, body=doc)
    except CosmosResourceNotFoundError:
        pass
    except Exception as exc:
        logging.warning("No se pudo marcar lock failed id=%s: %s", lock_id, exc)


def download_blob_bytes_and_metadata(
    *,
    connection_string: str,
    container_name: str,
    blob_path: str,
    size_hint: int | None,
) -> tuple[bytes, dict]:
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)
    source_blob_name = f"{container_name}/{blob_path}"

    metadata = {
        "source_blob_name": source_blob_name,
        "source_blob_url": blob_client.url,
        "blob_created_at": None,
        "blob_size_bytes": size_hint,
        "processed_at": datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat(),
    }

    downloader = blob_client.download_blob()
    image_bytes = downloader.readall()
    metadata["blob_size_bytes"] = len(image_bytes)

    try:
        props = blob_client.get_blob_properties()
        metadata["source_blob_url"] = blob_client.url
        metadata["blob_created_at"] = (
            props.creation_time.isoformat() if props.creation_time else None
        )
        if metadata["blob_size_bytes"] is None and props.size is not None:
            metadata["blob_size_bytes"] = int(props.size)
    except Exception as exc:
        logging.warning(
            "No fue posible leer propiedades completas del blob %s: %s",
            source_blob_name,
            exc,
        )

    return image_bytes, metadata


@app.event_grid_trigger(arg_name="event")
def visorapp(event: func.EventGridEvent):
    evt_type = _event_type_name(event)
    logging.info(
        "Event Grid: id=%s tipo=%s subject=%s",
        getattr(event, "id", None),
        evt_type,
        getattr(event, "subject", None),
    )

    parsed = parse_blob_created_from_event(event)
    if not parsed:
        return

    if (parsed["container"] or "").strip().lower() != IMAGES_CONTAINER:
        logging.info(
            "Contenedor %r no es %r; se ignora.",
            parsed["container"],
            IMAGES_CONTAINER,
        )
        return

    blob_url = parsed.get("url")
    if not blob_url:
        logging.warning("Evento sin URL de blob; no se puede descargar.")
        return

    blob_path = parsed["blob_path"]
    source_name_for_parser = f"{parsed['container']}/{blob_path}"
    parsed_name = parse_profile_image_filename(source_name_for_parser)
    if not parsed_name:
        logging.warning(
            "Nombre de imagen no cumple el formato esperado "
            "{email}~{utc_ts}~{uuid}{ext}. No se procesa: %s",
            source_name_for_parser,
        )
        return

    connection_string = _require_env("AzureProfileStorageSource")
    lock_id, lock_pk = _lock_id_and_pk(blob_url)

    cosmos_client = None
    locks = None
    try:
        cosmos_client = CosmosClient(
            _require_env("COSMOS_ENDPOINT"), _require_env("COSMOS_KEY")
        )
        locks = _locks_container_client(cosmos_client)
    except Exception as exc:
        logging.warning("Cosmos no disponible para locks: %s", exc)

    event_id = getattr(event, "id", None)
    if not try_begin_processing(
        locks,
        lock_id=lock_id,
        partition_key=lock_pk,
        blob_url=blob_url,
        container=parsed["container"],
        blob_path=blob_path,
        event_grid_id=event_id,
    ):
        return

    try:
        image_bytes, blob_metadata = download_blob_bytes_and_metadata(
            connection_string=connection_string,
            container_name=parsed["container"],
            blob_path=blob_path,
            size_hint=parsed.get("content_length"),
        )
        blob_metadata["uploader_email"] = parsed_name["email"]

        endpoint = _require_env("AZURE_AI_ENDPOINT")
        key = _require_env("AZURE_AI_KEY")
        document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=AzureKeyCredential(key)
        )
        poller = document_analysis_client.begin_analyze_document(
            "prebuilt-read", image_bytes
        )
        result = poller.result()

        extracted_text = []
        for page in result.pages:
            for line in page.lines:
                extracted_text.append(line.content)

        # print(extracted_text) # For debugging purposes

        stats = parse_pogo_data(extracted_text)

        # print(stats) # For debugging purposes

        team_result = detect_team_from_lateral_bands(image_bytes)
        stats["team"] = team_result["team"]
        stats["team_confidence"] = team_result["confidence"]
        save_to_cosmos(stats, blob_metadata)

        mark_processing_completed(locks, lock_id=lock_id, partition_key=lock_pk)
    except Exception as exc:
        logging.exception("Error procesando captura desde Event Grid: %s", exc)
        mark_processing_failed(
            locks,
            lock_id=lock_id,
            partition_key=lock_pk,
            message=str(exc),
        )
        raise


# --- Lógica portada de pokevisor (OCR, Cosmos player-stats, registered-users) ---

# Patrones de etiquetas: ES España (base), ES LATAM, EN (texto normalizado sin acentos).
def _label_alt(*parts: str) -> str:
    return "(?:" + "|".join(parts) + ")"


_LABEL_DISTANCIA_CAMINANDO = _label_alt(
    r"distancia\s+caminando",
    r"distancia\s+recorrida",
    r"distance\s+walked",
)
_LABEL_POKEMON_CAPTURADOS = _label_alt(
    r"pokemon\s+capturados?",
    r"pokemon\s+atrapados?",
    r"pokemon\s+caught",
)
_LABEL_POKEPARADAS_VISITADAS = _label_alt(
    r"pokeparadas\s+visitadas?",
    r"pokestops\s+visited",
)
_LABEL_TOTAL_EXPERIENCIA = _label_alt(
    r"total\s+de\s+px",
    r"total\s+de\s+exp",
    r"total\s+xp",
    r"total\s+de\s+px\s+x",
    r"total\s+xp\s+x",
    r"total\s+de\s+exp\s+x",
)
_LABEL_FECHA_INICIO = _label_alt(
    r"fecha\s+de\s+inicio",
    r"start\s+date",
    r"start\s+date\s+x",
    r"fecha\s+de\s+inicio\s+x",
)


def parse_pogo_data(lines):
    clean_lines = [line.strip() for line in lines if line and line.strip()]
    full_text = "\n".join(clean_lines)
    normalized_text = _normalize_text(full_text)
    logging.info("Texto OCR extraido:\n%s", full_text)

    date_locale = _detect_date_locale(normalized_text)
    player_name, buddy_name = _extract_player_and_buddy(clean_lines)

    level_match = re.search(r"\b(\d{1,3})\s*NIVEL\b", full_text, re.IGNORECASE)
    if not level_match:
        level_match = re.search(r"\bNIVEL\s*(\d{1,3})\b", full_text, re.IGNORECASE)
    if not level_match:
        level_match = re.search(r"\b(\d{1,3})\s*LEVEL\b", full_text, re.IGNORECASE)
    if not level_match:
        level_match = re.search(r"\bLEVEL\s*(\d{1,3})\b", full_text, re.IGNORECASE)
    level = int(level_match.group(1)) if level_match else None

    distancia_value = _extract_number_after_label(
        normalized_text,
        _LABEL_DISTANCIA_CAMINANDO,
        as_float=True,
        suffix_pattern=r"(?:\s*km)?",
    )
    pokemon_capturados = _extract_number_after_label(
        normalized_text, _LABEL_POKEMON_CAPTURADOS, as_float=False
    )
    pokeparadas_visitadas = _extract_number_after_label(
        normalized_text, _LABEL_POKEPARADAS_VISITADAS, as_float=False
    )
    total_px = _extract_number_after_label(
        normalized_text, _LABEL_TOTAL_EXPERIENCIA, as_float=False
    )
    fecha_raw = _extract_date_after_label(normalized_text, _LABEL_FECHA_INICIO)
    if not fecha_raw:
        fallback_date = re.search(r"(\d{1,2}/\d{1,2}/\d{4}|\d{2}/\d{2}/\d{4})", full_text)
        fecha_raw = fallback_date.group(1) if fallback_date else None
    fecha_inicio = _parse_start_date_to_iso(fecha_raw, date_locale)

    data = {
        "nombre_jugador": player_name,
        "pokemon_companero": buddy_name,
        "nivel": level,
        "numero_capturas": pokemon_capturados,
        "distancia_caminando_km": distancia_value,
        "total_pokemon_capturados": pokemon_capturados,
        "pokeparadas_visitadas": pokeparadas_visitadas,
        "experiencia_total": total_px,
        "fecha_inicio_juego": fecha_inicio,
        "missing_fields": [],
    }

    required_fields = [
        "nombre_jugador",
        "pokemon_companero",
        "nivel",
        "numero_capturas",
        "distancia_caminando_km",
        "total_pokemon_capturados",
        "pokeparadas_visitadas",
        "experiencia_total",
        "fecha_inicio_juego",
    ]
    data["missing_fields"] = [field for field in required_fields if data.get(field) in (None, "")]
    return data


def save_to_cosmos(stats, blob_info):
    url = _require_env("COSMOS_ENDPOINT")
    key = _require_env("COSMOS_KEY")

    client = CosmosClient(url, key)
    database = client.get_database_client(COSMOS_DATABASE)
    container = database.get_container_client("player-stats")

    player_name = stats.get("nombre_jugador") or _fallback_player_from_blob_name(
        blob_info.get("source_blob_name", "")
    )
    total_fields = 9
    extracted_fields = total_fields - len(stats.get("missing_fields", []))
    extraction_confidence = round(extracted_fields / total_fields, 3)

    document = {
        "id": str(uuid.uuid4()),
        "username": player_name,
        "stats": {
            "playerName": player_name,
            "buddyPokemonName": stats.get("pokemon_companero"),
            "level": stats.get("nivel"),
            "walkingDistanceKm": stats.get("distancia_caminando_km"),
            "totalPokemonCaptured": stats.get("total_pokemon_capturados"),
            "pokestopsVisited": stats.get("pokeparadas_visitadas"),
            "totalExperience": stats.get("experiencia_total"),
            "startDate": stats.get("fecha_inicio_juego"),
            "team": stats.get("team", "unknown"),
            "teamConfidence": stats.get("team_confidence", 0.0),
        },
        "extractionConfidence": extraction_confidence,
        "missingFields": stats.get("missing_fields", []),
        "metadata": {
            "source_blob_name": blob_info.get("source_blob_name"),
            "source_blob_url": blob_info.get("source_blob_url"),
            "blob_created_at": blob_info.get("blob_created_at"),
            "blob_size_bytes": blob_info.get("blob_size_bytes"),
            "processed_at": blob_info.get("processed_at"),
        },
    }

    container.create_item(body=document)
    logging.info("Historial guardado para %s", player_name)

    _try_update_registered_user_username(
        database=database,
        email=blob_info.get("uploader_email"),
        username=player_name,
    )


def _try_update_registered_user_username(database, email: str | None, username: str | None):
    if not email or not str(email).strip():
        logging.info("Sin email de uploader en metadata; se omite actualización de registered-users.")
        return
    if not username or not str(username).strip():
        logging.info(
            "Sin username extraído de stats para email %s; se omite actualización de registered-users.",
            email,
        )
        return

    email = str(email).strip()
    username = str(username).strip()

    try:
        users_container = database.get_container_client("registered-users")
    except Exception as exc:
        logging.warning(
            "No se pudo obtener el cliente del contenedor registered-users; se omite: %s",
            exc,
        )
        return

    try:
        users_container.read()
    except CosmosResourceNotFoundError:
        logging.warning(
            "El contenedor registered-users no existe en la base de datos; se omite actualización de username."
        )
        return
    except CosmosHttpResponseError as exc:
        if getattr(exc, "status_code", None) == 404:
            logging.warning(
                "El contenedor registered-users no está disponible (404); se omite actualización de username."
            )
            return
        logging.warning(
            "Error al leer metadatos del contenedor registered-users; se omite: %s",
            exc,
        )
        return
    except Exception as exc:
        logging.warning(
            "Error al validar el contenedor registered-users; se omite: %s",
            exc,
        )
        return

    query = "SELECT * FROM c WHERE c.email = @user_email"
    parameters = [{"name": "@user_email", "value": email}]
    try:
        items = list(
            users_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
            )
        )
    except CosmosResourceNotFoundError:
        logging.warning(
            "Consulta a registered-users falló (contenedor no encontrado); se omite actualización."
        )
        return
    except CosmosHttpResponseError as exc:
        if getattr(exc, "status_code", None) == 404:
            logging.warning(
                "Consulta a registered-users no encontró el recurso (404); se omite actualización."
            )
            return
        logging.warning("Error al consultar registered-users por email=%s: %s", email, exc)
        return
    except Exception as exc:
        logging.warning("Error al consultar registered-users por email=%s: %s", email, exc)
        return

    if not items:
        logging.info(
            "registered-users: no existe registro con email %s; se omite actualización de username.",
            email,
        )
        return

    user_doc = dict(items[0])
    user_doc["username"] = username

    try:
        users_container.replace_item(item=user_doc["id"], body=user_doc)
        logging.info(
            "registered-users: username actualizado a %r para email %s.",
            username,
            email,
        )
    except CosmosResourceNotFoundError:
        logging.warning(
            "replace_item en registered-users: ítem o contenedor no encontrado para email %s.",
            email,
        )
    except CosmosHttpResponseError as exc:
        logging.warning(
            "Error HTTP al actualizar registered-users (email=%s): %s",
            email,
            exc,
        )
    except Exception as exc:
        logging.warning(
            "Error al actualizar username en registered-users para email=%s: %s",
            email,
            exc,
        )


def parse_profile_image_filename(blob_path_or_name: str) -> dict | None:
    if not blob_path_or_name:
        return None
    file_name = blob_path_or_name.rstrip("/").split("/")[-1]
    if file_name.count("~") != 2:
        return None

    local_part, domain_and_rest = file_name.split("@", 1)
    if "@" not in file_name or not local_part or not domain_and_rest:
        return None

    after_at = domain_and_rest
    if "~" not in after_at:
        return None
    domain, tail = after_at.split("~", 1)
    email = f"{local_part}@{domain}"
    if "~" not in tail:
        return None
    utc_ts, uuid_and_ext = tail.split("~", 1)

    if not re.fullmatch(r"\d{8}T\d{6}Z", utc_ts or ""):
        return None

    ext_match = re.search(r"(\.[A-Za-z0-9]{1,10})$", uuid_and_ext or "")
    if not ext_match:
        return None
    ext = ext_match.group(1).lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".heic", ".heif"):
        return None

    uuid_part = uuid_and_ext[: -len(ext)]
    uuid_ok = bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            uuid_part,
        )
        or re.fullmatch(r"[0-9a-fA-F]{32}", uuid_part)
    )
    if not uuid_ok:
        return None

    if not re.fullmatch(r"[^@\s]+@[^@\s~]+\.[^@\s~]+", email):
        return None

    return {
        "email": email,
        "utc_ts": utc_ts,
        "uuid": uuid_part,
        "extension": ext,
    }


def detect_team_from_lateral_bands(image_bytes: bytes) -> dict:
    try:
        from PIL import Image
    except ImportError:
        logging.warning("Pillow no disponible; team=unknown")
        return {"team": "unknown", "confidence": 0.0}

    try:
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception as exc:
        logging.warning("No se pudo decodificar imagen para deteccion de team: %s", exc)
        return {"team": "unknown", "confidence": 0.0}

    width, height = image.size
    if width < 48 or height < 48:
        return {"team": "unknown", "confidence": 0.0}

    strip_px = max(4, int(width * 0.022))
    margin_y = int(height * 0.05)
    y0 = margin_y
    y1 = height - margin_y
    if y1 <= y0 + 8:
        y0, y1 = 0, height

    left_box = (0, y0, strip_px, y1)
    right_box = (width - strip_px, y0, width, y1)

    left_rgb = _strip_mean_rgb(image, left_box)
    right_rgb = _strip_mean_rgb(image, right_box)

    left_team, left_conf = _classify_team_from_strip_rgb(*left_rgb)
    right_team, right_conf = _classify_team_from_strip_rgb(*right_rgb)

    logging.info(
        "Team strips RGB: left=%s right=%s -> left=%s(%.2f) right=%s(%.2f)",
        tuple(round(x, 1) for x in left_rgb),
        tuple(round(x, 1) for x in right_rgb),
        left_team or "-",
        left_conf,
        right_team or "-",
        right_conf,
    )

    if left_team and left_team == right_team:
        conf = min(1.0, (left_conf + right_conf) / 2.0)
        return {"team": left_team, "confidence": round(conf, 3)}

    if left_team and not right_team:
        return {"team": left_team, "confidence": round(min(1.0, left_conf * 0.85), 3)}
    if right_team and not left_team:
        return {"team": right_team, "confidence": round(min(1.0, right_conf * 0.85), 3)}

    if left_team and right_team and left_team != right_team:
        mx = (left_rgb[0] + right_rgb[0]) / 2.0
        my = (left_rgb[1] + right_rgb[1]) / 2.0
        mz = (left_rgb[2] + right_rgb[2]) / 2.0
        merged_team, merged_conf = _classify_team_from_strip_rgb(mx, my, mz)
        if merged_team:
            return {"team": merged_team, "confidence": round(min(1.0, merged_conf * 0.75), 3)}

    return {"team": "unknown", "confidence": 0.0}


def _strip_mean_rgb(image, box):
    from PIL import ImageStat

    region = image.crop(box)
    if region.size[0] < 1 or region.size[1] < 1:
        return (0.0, 0.0, 0.0)
    stat = ImageStat.Stat(region)
    return (float(stat.mean[0]), float(stat.mean[1]), float(stat.mean[2]))


def _classify_team_from_strip_rgb(r: float, g: float, b: float):
    yellow_lift = (r + g) / 2.0 - b
    if yellow_lift >= 22.0 and r >= 88.0 and g >= 88.0 and b <= min(r, g) - 12.0:
        conf = min(1.0, yellow_lift / 75.0)
        return "Instinct", conf

    if b >= max(r, g) + 14.0 and b >= 95.0:
        conf = min(1.0, (b - max(r, g) + 20.0) / 95.0)
        return "Mystic", conf

    if r >= max(g, b) + 12.0 and (r - b) >= 28.0 and r >= 100.0:
        conf = min(1.0, (r - b) / 95.0)
        return "Valor", conf

    return None, 0.0


def _require_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _normalize_text(text):
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return text


def _detect_date_locale(normalized_text: str) -> str:
    lowered = normalized_text.lower()
    latam_markers = (
        "distancia recorrida",
        "pokemon atrapados",
        "total de exp",
    )
    if any(marker in lowered for marker in latam_markers):
        return "latam"
    return "mmdd"


def _parse_start_date_to_iso(raw: str | None, locale: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    parts = [p.strip() for p in raw.split("/")]
    if len(parts) != 3:
        return None
    p0, p1, p2 = parts[0], parts[1], parts[2]
    if not (p0.isdigit() and p1.isdigit() and p2.isdigit() and len(p2) == 4):
        return None
    year = int(p2)
    try:
        if locale == "latam":
            day, month = int(p0), int(p1)
            if not (1 <= month <= 12 and 1 <= day <= 31):
                return None
            return datetime.date(year, month, day).isoformat()
        month, day = int(p0), int(p1)
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        return datetime.date(year, month, day).isoformat()
    except ValueError:
        return None


def _extract_player_and_buddy(lines):
    filtered_lines = [line.strip() for line in lines if line and line.strip()]
    buddy_index = None
    buddy_name = None
    player_name = None

    for idx, line in enumerate(filtered_lines):
        same_line = re.search(
            r"^(.+?)\s*&\s*([A-Za-z0-9À-ÿ' -]{2,})$",
            line,
            re.IGNORECASE,
        )
        if same_line:
            cand_player = same_line.group(1).strip()
            cand_buddy = same_line.group(2).strip()
            if _is_probable_player_name(cand_player) and cand_buddy:
                player_name = cand_player
                buddy_name = cand_buddy
                return player_name, buddy_name

    for idx, line in enumerate(filtered_lines):
        buddy_match = re.search(r"^\s*&\s*([A-Za-z0-9À-ÿ' -]{2,})$", line)
        if not buddy_match:
            buddy_match = re.search(r"\s*&\s*([A-Za-z0-9À-ÿ' -]{2,})$", line)
        if not buddy_match:
            buddy_match = re.search(
                r"\bcon\s+([A-Za-z0-9À-ÿ' -]{2,})$", line, re.IGNORECASE
            )
        if buddy_match:
            buddy_index = idx
            buddy_name = buddy_match.group(1).strip()
            break

    if buddy_index is not None and buddy_index > 0:
        candidate = filtered_lines[buddy_index - 1]
        if _is_probable_player_name(candidate):
            player_name = candidate

    if not player_name:
        for line in filtered_lines:
            if _is_probable_player_name(line):
                player_name = line
                break

    return player_name, buddy_name


def _is_probable_player_name(line):
    line_norm = _normalize_text(line).lower()
    blocked_words = [
        "Vo",
        "LTE"
        "H+",
        "5G"
        "VPN",
        "yo",
        "me",
        "amigos",
        "friends",
        "social",
        "nivel",
        "level",
        "historial",
        "buddy history",
        "historial de compañeros",
        "album",
        "album de recuerdos",
        "scrapbook",
        "diario",
        "journal",
        "personalizar",
        "style",
        "distancia caminando",
        "distancia recorrida",
        "distance walked",
        "pokemon capturados",
        "pokemon atrapados",
        "pokemon caught",
        "pokeparadas visitadas",
        "pokestops visited",
        "total de px",
        "total de exp",
        "total xp",
        "fecha de inicio",
        "start date",
        "con ",
    ]

    if any(word in line_norm for word in blocked_words):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_ .'-]{3,25}", line.strip()))


def _extract_number_after_label(text, label_pattern, as_float=False, suffix_pattern=""):
    pattern = (
        rf"{label_pattern}\s*(?::|-)?\s*(?:\n|\s)+([0-9][0-9\.,]*){suffix_pattern}"
    )
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    value = match.group(1)
    return _parse_localized_number(value, as_float=as_float)


def _extract_date_after_label(text, label_pattern):
    pattern = rf"{label_pattern}\s*(?::|-)?\s*(?:\n|\s)+(\d{{1,2}}/\d{{1,2}}/\d{{4}})"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def _parse_localized_number(raw, as_float=False):
    cleaned = re.sub(r"[^\d,\.]", "", raw or "")
    if not cleaned:
        return None

    if not as_float:
        digits = "".join(ch for ch in cleaned if ch.isdigit())
        return int(digits) if digits else None

    value = cleaned
    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        trailing = value.split(",")[-1]
        if len(trailing) <= 2:
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "." in value:
        trailing = value.split(".")[-1]
        if len(trailing) > 2:
            value = value.replace(".", "")

    try:
        return float(value)
    except ValueError:
        return None


def _fallback_player_from_blob_name(blob_name):
    blob_file_name = blob_name.split("/")[-1] if blob_name else ""
    file_without_ext = os.path.splitext(blob_file_name)[0]
    return file_without_ext.split("_")[0] if file_without_ext else "unknown-player"
