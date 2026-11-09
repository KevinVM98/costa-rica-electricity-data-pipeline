# Modelos de generación eléctrica con dbt

dbt transforma el histórico que Airflow carga en `raw.generacion_planta_v2`. Los modelos se ejecutan en PostgreSQL y se publican en `dbt_dev_staging` y `dbt_dev_mart`.

| Modelo | Una fila representa | Uso |
| --- | --- | --- |
| `stg_generacion_planta` | Un contenido único de RAW | Tipar las mediciones y rastrear cada registro hasta su origen. |
| `mart_generacion_mensual_planta` | Una combinación de empresa, planta y mes | Analizar tendencias por planta, armar rankings y revisar períodos con datos incompletos. |
| `mart_generacion_mensual` | Un mes observado en el conjunto | Consultar la serie mensual y ver cuántos períodos aportaron energía al total. |

`source()` declara la tabla de entrada. `ref()` conecta los modelos entre sí y determina el orden de construcción. Las descripciones en YAML documentan el significado de cada columna.

## Ejecutar desde Airflow

Después de `cargar_raw`, la tarea `transformar_datos` actualiza las relaciones de compatibilidad con `sql/transformaciones.sql` y ejecuta `dbt build` con el mismo PostgreSQL de la carga. El worker reutiliza las variables `ICE_PG_*` de `airflow/.env`. El código de salida de dbt se refleja en el resultado de la tarea. Si un modelo o una prueba falla, la tarea entra en el flujo de reintentos de Airflow.

La imagen `aresep-airflow:3.3.2-dbt` incluye los modelos y las dependencias de `dbt/requirements.txt` en un entorno Python separado. Después de editar un modelo o su configuración, volvé a construir la imagen y actualizá el worker de la instalación que estés usando. Para la instalación local predeterminada, ejecutá lo siguiente desde `airflow/` con los demás servicios iniciados.

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml build airflow-worker
docker compose -f docker-compose.yaml -f compose.override.yaml up -d --no-deps --no-build airflow-dag-processor airflow-worker
```

En Airflow, abrí `generacion_electrica_aresep` y revisá el log de `transformar_datos` después de una ejecución en modo `mensual`. El proyecto actual tiene tres modelos y once pruebas, por lo que el resumen esperado de dbt es `PASS=14 ERROR=0`. En el clon paralelo, usá el proyecto Compose `aresep-airflow-prueba` y su interfaz de Airflow en el puerto `8081`.

## Cómo interpretar los totales

- El total por planta y mes suma punta, valle y noche si hay un único registro con las tres mediciones.
- Los períodos con varios contenidos se marcan como `MULTIPLES_REGISTROS`. Los períodos con un registro y mediciones faltantes se marcan como `SIN_ENERGIA_COMPLETA`. Ambos quedan con energía nula.
- El MART mensual suma los períodos utilizables y deja el total nulo si ninguno aporta energía. Los conteos describen las combinaciones observadas. La cobertura de meses y plantas depende de la descarga de ARESEP.
- Las columnas de energía conservan la escala de los campos originales. El histórico consultado de 2018 a 2026 trae empresa y planta. Para desglosar por tecnología de generación se requiere un catálogo de centrales con ese atributo.

## Ejecutar manualmente en Windows

Para esta prueba, necesitás Python 3.12 a 3.14, PostgreSQL iniciado y una carga histórica de Airflow en la base elegida. El perfil predeterminado apunta a `127.0.0.1:5433`, base `ice_data`, usuario `ice_user`. Para usar el clon de prueba, indicá `5434` como `ARESEP_DBT_PG_PORT` y usá la contraseña de ese PostgreSQL. El perfil también lee `ARESEP_DBT_PG_HOST`, `ARESEP_DBT_PG_DB` y `ARESEP_DBT_PG_USER` si necesitás cambiar esos valores.

Desde la raíz del repositorio, en PowerShell, creá un entorno separado para dbt e instalá las versiones declaradas.

```powershell
python -m venv .venv-dbt
.\.venv-dbt\Scripts\python.exe -m pip install -r .\dbt\requirements.txt
```

En la misma terminal, ingresá la contraseña de PostgreSQL. PowerShell oculta lo que tecleás y dbt lee el valor desde la variable de entorno de la sesión.

```powershell
$env:DBT_ENV_SECRET_PG_PASSWORD = [System.Net.NetworkCredential]::new(
    '', (Read-Host 'Contraseña de PostgreSQL' -AsSecureString)
).Password

$dbtExe = '.\.venv-dbt\Scripts\dbt.exe'
$dbtOpciones = @('--project-dir', '.\dbt', '--profiles-dir', '.\dbt')
& $dbtExe debug @dbtOpciones
& $dbtExe build @dbtOpciones
```

`debug` comprueba la conexión. `build` ejecuta los modelos y sus pruebas de datos en orden de dependencia. Las pruebas cubren la presencia de datos en RAW, las claves, el tratamiento de períodos ambiguos y la consistencia de los conteos mensuales.

Para consultar los seis meses más recientes y luego abrir el linaje en el navegador, ejecutá lo siguiente.

```powershell
& $dbtExe show @dbtOpciones --inline "select * from {{ ref('mart_generacion_mensual') }} order by mes desc" --limit 6
& $dbtExe docs generate @dbtOpciones
& $dbtExe docs serve @dbtOpciones --host 127.0.0.1 --port 8082
```

Abrí [dbt Docs](http://localhost:8082) y seleccioná `mart_generacion_mensual` para explorar su linaje. Detené el servidor de documentación con `Ctrl+C`.
