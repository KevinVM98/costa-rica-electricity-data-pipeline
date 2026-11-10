# Almacenamiento de ARESEP en MinIO y Parquet

El DAG `generacion_electrica_aresep` conserva las respuestas JSON de ARESEP
en MinIO y publica snapshots Parquet del STAGING de dbt. PostgreSQL sigue
siendo la base de RAW, monitoreo y modelos analiticos.

## Responsabilidades de las capas

| Ubicacion | Contenido | Uso |
| --- | --- | --- |
| MinIO `raw/generacion_planta/` | Cuerpo JSON original, incluida metadata, por ano y ejecucion | Auditoria y recuperacion de la fuente recibida |
| PostgreSQL `raw.generacion_planta_v2` | Registros JSONB distintos por hash | Historico de registros y fuente de dbt |
| PostgreSQL `dbt_dev_staging.stg_generacion_planta` | Vista con nombres y tipos normalizados por dbt | Contrato de datos para los modelos |
| MinIO `processed/generacion_planta/` | Snapshot Parquet del STAGING, separado por ano | Consumo en Python, motores analiticos o una futura implementacion con Spark |
| PostgreSQL `dbt_dev_mart.*` | Modelos mensuales analiticos de dbt | Consultas y futura conexion con BI |

PROCESSED es una representacion portable del STAGING existente, no una
segunda implementacion de sus transformaciones. Conserva el grano de RAW:
una fila por documento distinto. No se debe interpretar cada fila como un
periodo planta/empresa unico; esa resolucion corresponde a los MARTs.

## Flujo del DAG

1. `registrar_inicio`: registra la ejecucion en PostgreSQL.
2. `extraer_generacion`: descarga los anos seleccionados y guarda cada cuerpo
   de respuesta en MinIO antes de validar su estructura JSON. Devuelve solo
   referencias a los objetos mediante XCom.
3. `validar_datos`: lee esos objetos, comprueba SHA-256 y aplica las reglas
   existentes de `calidad.py`.
4. `cargar_raw`: lee las mismas referencias y realiza la carga idempotente
   existente por `contenido_hash`. Conserva las metricas de carga.
5. `transformar_datos`: ejecuta las transformaciones de compatibilidad y
   `dbt build`. Un fallo de dbt bloquea la exportacion.
6. `exportar_parquet`: obtiene una lectura consistente del STAGING completo,
   valida el contrato, genera un Parquet por ano y lo verifica desde MinIO.
   Publica el manifiesto al finalizar.

El DAG mantiene `max_active_runs=1`. Los callbacks marcan el resultado de
la ejecucion completa, incluida la exportacion.

## Organizacion de los objetos

Las rutas RAW tienen la forma:

`raw/generacion_planta/ejecucion=<id>/anho=<ano>/respuesta_<sha256>.json`

Las rutas Parquet tienen la forma:

`processed/generacion_planta/ejecucion=<id>/anho=<ano>/part_<sha256>.parquet`

El identificador `<id>` es un SHA-256 estable del DAG y su `run_id`.
El `manifest.json` de cada ejecucion incluye el `run_id` legible, las rutas,
los hashes y los conteos. No contiene credenciales.

Un reintento con el mismo contenido usa las mismas claves. Una respuesta
diferente obtiene otra clave y conserva la anterior. Los archivos parciales
de un intento fallido pueden quedar almacenados para su inspeccion.

La extraccion publica su propio manifiesto cuando termino de descargar y
comprobar la estructura de los anos solicitados; ese manifiesto **no** afirma
que ya se hayan superado la validacion de calidad ni la carga a PostgreSQL.

## Como consumir el snapshot publicado

El punto de entrada es:

`s3://aresep-data/processed/generacion_planta/latest.json`

Su lista `archivos` identifica exactamente los Parquet del ultimo snapshot
publicado correctamente. Se actualiza cuando todos sus archivos pasaron las
comprobaciones. El manifiesto por ejecucion tambien se conserva.

Leer exclusivamente los archivos indicados en un manifiesto. Leer todo el
prefijo PROCESSED mezclaria snapshots y repetiria registros. Cada archivo
incluye la columna `anho`; para leerlo individualmente con PyArrow puede
usarse `pq.read_table(pa.BufferReader(contenido))`, como hace el validador,
sin inferir de nuevo el tipo de la particion a partir de la ruta.

El modo `mensual` descarga el ano actual y el anterior. El modo `historico`
descarga desde 2018 hasta el ano actual. En ambos casos, la exportacion
Parquet representa **todo el STAGING presente en PostgreSQL**, incluidos
anos cargados antes. La primera ejecucion de esta ampliacion conviene
hacerla en modo `historico` para archivar tambien las respuestas antiguas.

## Validacion visible

Implementada en `airflow/dags/almacenamiento.py`, con checks propios y PyArrow:

- Columnas exactas del contrato de exportacion v1. Una columna faltante o
  adicional requiere revisar el contrato antes de publicar.
- Identificadores, ano, mes, empresa, planta y hash obligatorios.
- Ano entre 2018 y 9999, mes entre 1 y 12, identificadores positivos.
- `raw_id` y `contenido_hash` unicos en el snapshot.
- Energia y factor de carga como `Decimal` finito o nulo.
- Esquema Parquet explicito. Los cuatro campos numericos usan
  `decimal128(38,12)`: hasta 26 digitos enteros y 12 decimales. Este es el
  contrato elegido para exportar, no una afirmacion de la precision de la API.
  Un valor que no pueda representarse exactamente provoca un error, sin
  redondearlo silenciosamente.
- `Table.validate(full=True)` y escritura con compresion Snappy.
- Descarga del objeto subido: verificacion de longitud, SHA-256, esquema y
  todos los valores contra la tabla original.
- El manifiesto PROCESSED y `latest.json` se publican despues de validar
  todas las particiones; un fallo previo conserva el ultimo snapshot publicado.

No se rellenan meses faltantes ni se garantiza que ARESEP haya publicado
todos los meses o plantas. Los checks validan lo recibido y lo exportado.

## Configuracion

En `airflow/.env`:

```dotenv
S3_ENDPOINT_URL=http://host.docker.internal:9000
S3_BUCKET=aresep-data
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=tu_usuario_de_minio
S3_SECRET_ACCESS_KEY=tu_clave_de_minio
```

Las credenciales reales permanecen en `.env`. `.env.example` contiene
ejemplos. La licencia de AIStor permanece en `secrets/`, excluido de Git.

Las dependencias utilizadas son `boto3==1.43.75`, `botocore==1.43.75` y
`pyarrow==26.0.1`. Esos pines de Boto3 y Botocore respetan el rango del
`aiobotocore` instalado en la imagen Airflow 3.3.2 de este proyecto.

## Pruebas

```powershell
.\.venv\Scripts\python.exe -m pip install pytest -r requirements.txt
.\.venv\Scripts\python.exe -m pytest tests/test_almacenamiento.py -q
```

Las pruebas usan el codigo real, archivos Parquet reales y un doble de S3
en memoria. Cubren integridad, precision, nulos, cambios de esquema,
reintentos, particiones y fallos que no deben publicar snapshots incompletos.
GitHub Actions instala las dependencias de `requirements.txt` y ejecuta
tambien las pruebas anteriores, con PostgreSQL temporal para las de integracion.

Las pruebas unitarias no reemplazan la ejecucion completa contra MinIO,
ARESEP, PostgreSQL y Airflow. Esa validacion se realiza al ejecutar el DAG
y comprobar la tarea `exportar_parquet` y su manifiesto en el bucket.

## Alcance y limites

La exportacion carga el STAGING completo en memoria, apropiado para el
tamano actual del proyecto. Para volumenes grandes habria que migrar a
lectura por lotes y escritura incremental.

PostgreSQL y MinIO no comparten una transaccion. Si falla la exportacion,
los modelos de PostgreSQL pueden haberse actualizado, mientras `latest.json`
sigue describiendo el snapshot anterior. Airflow marca el fallo y permite
reintentar. La publicacion del manifiesto no constituye una transaccion
distribuida ni una funcion de Delta Lake.

Conservar el manifiesto hace consumible una version completa. Por ahora
no se aplica borrado automatico de snapshots ni de objetos de intentos
fallidos; se puede anadir una politica de retencion cuando se defina su plazo.

## Resultado de la ejecucion local del 10 de noviembre de 2026

El resumen se obtuvo leyendo `processed/generacion_planta/latest.json`
desde el worker de Airflow, despues de completar `exportar_parquet`.

- Ejecucion: `manual__2026-11-10T20:05:44.523763+00:00`.
- Filas publicadas: **5.819**.
- Archivos Parquet: **9**.

| Ano | Filas |
| --- | ---: |
| 2018 | 292 |
| 2019 | 692 |
| 2020 | 658 |
| 2021 | 663 |
| 2022 | 733 |
| 2023 | 753 |
| 2024 | 648 |
| 2025 | 815 |
| 2026 | 565 |
| **Total** | **5.819** |

Esta ejecucion confirma la integracion en el entorno local existente.
La instalacion limpia de toda la ampliacion MinIO/Parquet todavia no se ha
repetido en un clon independiente. Los conteos describen este snapshot,
no una cantidad fija que deba mantenerse cuando la fuente cambie.

## Referencias tecnicas

- https://arrow.apache.org/docs/python/parquet.html
- https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
- https://arrow.apache.org/docs/python/generated/pyarrow.Table.html
- https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html
