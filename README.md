# Visorapp

Azure Function en Python que procesa **capturas de pantalla del perfil de Pokémon GO** almacenadas en Azure Blob Storage. El disparador es **Microsoft Event Grid** (`Microsoft.Storage.BlobCreated`): al subir una imagen al contenedor configurado se descarga el blob, se extrae texto con **Azure AI Document Intelligence** (modelo prebuilt-read), se interpretan estadísticas del jugador, se intenta inferir el **equipo** (Instinct / Mystic / Valor) a partir del color de bandas laterales en la imagen y se persisten los resultados en **Azure Cosmos DB**.

## Requisitos

- Python **3.10 o superior** (recomendado para el modelo de programación v2 de Functions).
- [Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local) instalado (`func`).
- Una suscripción/cuentas de Azure con:
  - **Storage Account** (cadena de conexión con acceso al blob procesado).
  - **Cosmos DB** (endpoint + clave).
  - Recurso **Azure AI Document Intelligence** (endpoint + clave; en código se usa el cliente `azure-ai-formrecognizer` contra el servicio actual de análisis de documentos).

## Instalación local

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate      # Linux / macOS
pip install -r requirements.txt
```

Crea **`local.settings.json`** en la raíz del proyecto (no se versiona; está en `.gitignore`) con valores reales para tu entorno. Las claves necesarias están en la tabla siguiente dentro de `"Values"`:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureProfileStorageSource": "<cadena-conexion-storage-del-blob-de-capturas>",
    "COSMOS_ENDPOINT": "https://<cuenta>.documents.azure.com:443/",
    "COSMOS_KEY": "<clave-primaria-o-secundaria>",
    "AZURE_AI_ENDPOINT": "https://<recurso>.cognitiveservices.azure.com/",
    "AZURE_AI_KEY": "<clave-document-intelligence>"
  }
}
```

Para Storage local con Azurite, ajusta `AzureWebJobsStorage` y/o la cadena que uses para el origen según tu configuración.

## Variables de entorno

| Variable | Obligatoria | Descripción |
|----------|-------------|-------------|
| `AzureProfileStorageSource` | Sí | Cadena de conexión del Blob Storage donde están las capturas (se usa para descargar el archivo). |
| `COSMOS_ENDPOINT` | Sí | Endpoint de Cosmos DB. |
| `COSMOS_KEY` | Sí | Clave de Cosmos. |
| `AZURE_AI_ENDPOINT` | Sí | Endpoint del servicio de Azure AI Document Intelligence. |
| `AZURE_AI_KEY` | Sí | Clave del servicio de análisis de documentos. |
| `AzureWebJobsStorage` | Local / despliegue | Cadena del storage usado por el runtime de Functions (persistencia interna). |
| `FUNCTIONS_WORKER_RUNTIME` | Sí | Debe ser `python`. |
| `VISORAPP_IMAGES_CONTAINER` | No | Nombre del contenedor de blobs a procesar. Por defecto: `images`. |
| `COSMOS_DATABASE` | No | Base de datos Cosmos. Por defecto: `pogo-db`. |
| `COSMOS_LOCKS_CONTAINER` | No | Contenedor para idempotencia (locks por blob). Por defecto: `image-processing-locks`. El contenedor debe existir en la misma cuenta, con **partition key** `/pk`. |
| `VISORAPP_PROCESSING_STALE_AFTER_SECONDS` | No | Tras ese tiempo en estado `in_progress`, otro intento puede retomar el lock (p. ej. worker colgado). Por defecto: `900`. |
| `VISORAPP_DISABLE_PROCESSING_LOCKS` | No | Si es `true` / `1` / `yes`, no se usa deduplicación por locks en Cosmos (útil solo para diagnóstico). |

## Contenedores y datos en Cosmos DB

La aplicación espera estos recursos en la base indicada por `COSMOS_DATABASE`:

- **`player-stats`**: aquí se inserta un documento por procesamiento exitoso con el historial OCR, nivel de confianza de extracción, metadatos del blob y el campo `stats` (nombre, nivel, XP, equipo, etc.).
- **`image-processing-locks`** (opcional pero recomendado): documentos de control para evitar trabajo duplicado ante reintentos de Event Grid; partición **`/pk`**.
- **`registered-users`** (opcional): si existe, se intenta actualizar el campo `username` del documento cuyo `email` coincide con el del nombre del blob (véase formato de archivo).

En producción debe existir **`player-stats`**. Si faltan los otros contenedores, el código continúa con advertencias en logs (sin bloquear el flujo principal de estadísticas).

## Formato del nombre del blob

Solo se procesan blobs cuyo **nombre de archivo** sigue este patrón (el archivo puede estar en subcarpetas dentro del contenedor):

`{usuario}@{dominio}~{YYYYMMDD}THHMMSSZ~{uuid}{ext}`

- `usuario@dominio`: email del jugador que subió la captura.
- Segmento temporal en UTC según la expresión regular `YYYYMMDDHHMMSS` + `Z`.
- UUID en formato estándar con guiones o cadena hexadecimal de 32 caracteres.
- Extensión permitida: `.jpg`, `.jpeg`, `.png`, `.webp`, `.gif`, `.bmp`, `.heic`, `.heif`.

Ejemplo: `trainer@ejemplo.com~20260427T150000Z~a1b2c3d4-e5f6-7890-abcd-ef1234567890.jpg`

El contenedor del blob debe coincidir con `VISORAPP_IMAGES_CONTAINER` (por defecto `images`).

## Ejecución local

Desde la raíz del proyecto, con dependencias instaladas y `local.settings.json` configurado:

```bash
func start
```

La función **`visorapp`** está registrada como disparador **Event Grid**. En local, suele estar disponible en una URL como:

`http://localhost:7071/runtime/webhooks/EventGrid?functionName=visorapp`

### Simular BlobCreated contra la función local

Hay un script de ayuda en `scripts/send_eventgrid_blobcreated.py`:

```bash
python scripts/send_eventgrid_blobcreated.py ^
  --blob-url "https://<cuenta>.blob.core.windows.net/images/trainer@correo.com~20260427T150000Z~a1b2c3d4-e5f6-7890-abcd-ef1234567890.jpg"
```

En PowerShell puedes usar comillas simples exterior o escapar según necesites. Opciones útiles:

- `--endpoint`: URL completa del webhook Event Grid local (valor por defecto apunta a `functionName=visorapp`).
- `--timeout`: tiempo máximo HTTP en segundos.

El blob referenciado en `--blob-url` debe existir en Storage y la función debe poder descargarlo con `AzureProfileStorageSource`.

## Despliegue en Azure

1. Publica la Function App (consumo / premium / plan según necesidad).
2. Configura en **Application settings** las mismas variables que en `local.settings.json` (`Values`).
3. Crea una **suscripción de Event Grid** desde el recurso Storage (evento Blob Created) al **punto del sistema Topic** de tu Function App o al endpoint HTTPS del webhook de Event Grid, filtrando por el contenedor (`images` u otro configurado).

La suscripción debe enviar solo eventos **`Microsoft.Storage.BlobCreated`** compatibles con el esquema que parsea `parse_blob_created_from_event` en `function_app.py` (campo `data.url` y/o `subject` con rutas típicas de Storage).

## Estructura del repositorio

| Ruta | Contenido |
|------|-----------|
| `function_app.py` | Aplicación de Functions: disparador Event Grid, OCR, parsers, equipo, escritura Cosmos. |
| `host.json` | Configuración del host (`extensionBundle`, logging / Application Insights). |
| `requirements.txt` | Dependencias de Python (`azure-functions`, SDKs Azure, Pillow). |
| `scripts/send_eventgrid_blobcreated.py` | Utilidad HTTP para disparar manualmente BlobCreated contra entorno local. |

## Dependencias principales

- **azure-functions**: runtime del modelo de programación v2.
- **azure-ai-formrecognizer**: cliente para Document Intelligence (`prebuilt-read`).
- **azure-cosmos**, **azure-storage-blob**: persistencia y descarga del blob.
- **Pillow**: análisis de color en bordes de la captura para el equipo Pokémon.

---

Si amplías los esquemas de Cosmos o el contrato Event Grid en producción, actualiza también este README y los comentarios de configuración junto al código fuente del disparador.
