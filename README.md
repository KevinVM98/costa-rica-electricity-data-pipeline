# Costa Rica Electricity Generation Data Pipeline

Pipeline **ELT de ingeniería de datos** para recopilar, validar, almacenar y preparar para análisis información pública de generación eléctrica por planta en Costa Rica, utilizando la API de datos abiertos de la **Autoridad Reguladora de los Servicios Públicos (ARESEP)**.

El proyecto combina **Python, PostgreSQL, Apache Airflow, dbt, SQL y Docker Compose** en una plataforma local que admite cargas históricas e incrementales, controles de calidad y monitoreo operativo.

> **Alcance:** proyecto personal y educativo de ingeniería de datos. No es una plataforma oficial de ARESEP ni del Instituto Costarricense de Electricidad (ICE). La infraestructura Docker incluida está orientada a desarrollo y demostraciones locales, no a producción.

## 1. Objetivos

- Extraer datos públicos de generación eléctrica por planta desde una API REST.
- Mantener un histórico de registros sin volver a insertar contenidos idénticos.
- Separar el almacenamiento en capas **RAW**, **STAGING** y **MART**.
- Orquestar el proceso con tareas, reintentos y ejecuciones programadas en Airflow.
- Validar los datos antes de cargarlos y conservar métricas de cada ejecución.
- Permitir que el entorno pueda reconstruirse desde una clonación limpia del repositorio.

## 2. Stack tecnológico

| Tecnología | Función |
| --- | --- |
| Python | Consumo de la API, validaciones y carga |
| Apache Airflow 3.3.2 | Orquestación del pipeline |
| dbt Core | Modelos analíticos y pruebas de datos en PostgreSQL |
| PostgreSQL 17 | Almacenamiento del conjunto de datos ARESEP |
| PostgreSQL 16 | Base interna de metadatos de Airflow |
| SQL | Limpieza, transformación y consultas |
| Docker y Docker Compose | Contenedores, redes y volúmenes |
| Redis | Broker de tareas del CeleryExecutor de Airflow |
| Git y GitHub | Control de versiones y publicación |

La imagen de Airflow se construye a partir de `airflow/Dockerfile`, con las dependencias necesarias para ejecutar el DAG.

## 3. Arquitectura

```text
                         API REST de ARESEP
                                 |
                                 v
                         Apache Airflow
                                 |
             +-------------------+------------------+
             | 1. registrar_inicio                  |
             | 2. extraer_generacion                |
             | 3. validar_datos                     |
             | 4. cargar_raw                        |
             | 5. transformar_datos                 |
             +-------------------+------------------+
                                 |
                                 v
                       PostgreSQL (ARESEP)
                                 |
                    +------------+------------+
                    |            |            |
                   RAW        STAGING        MART
                  JSONB      SQL tipado    Vista SQL
                    |            |            |
                    +------------+------------+
                                 |
                       Consumo analítico
                        (fase posterior)

              Monitoreo operativo en PostgreSQL
              - monitoring.ejecuciones_pipeline
              - monitoring.calidad_datos
              - monitoring.carga_datos

       Infraestructura interna de Airflow (separada)
             PostgreSQL 16 + Redis + servicios Airflow
```

**Importante:** la base de datos interna de Airflow almacena metadatos del orquestador; no debe confundirse con el PostgreSQL 17 que conserva los registros de ARESEP.

## 4. Estructura del repositorio

```text
costa-rica-electricity-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── generacion_aresep.py
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── compose.override.yaml
│   └── .env.example
├── dbt/                   # Modelos analíticos, documentación y pruebas de datos
├── sql/
│   ├── init.sql
│   └── transformaciones.sql
├── compose.yaml
├── descargar_generacion.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

Los archivos `.env`, las credenciales, los registros descargados y los volúmenes de bases de datos **no se publican** en el repositorio.

## 5. Origen y cobertura de los datos

- **Proveedor:** ARESEP, portal de datos abiertos de Costa Rica.
- **Recurso:** `ObtenerGeneracionFuentePlantaYCapacidadInstalada`.
- **Formato consumido:** JSON.
- **Cobertura comprobada:** registros correspondientes a 2018–2026.
- **Disponibilidad parcial observada:** 2018 comienza en agosto y los registros consultados de 2026 llegan hasta septiembre. La cobertura puede cambiar con futuras actualizaciones de la API.

La consulta se realiza por año mediante el endpoint:

```text
https://datos.aresep.go.cr/ws.datosabiertos/Services/IE/Electricidad.svc/ObtenerGeneracionFuentePlantaYCapacidadInstalada/{anio}
```

La fuente es externa: la disponibilidad, estructura y cantidad de registros pueden cambiar con el tiempo.

## 6. Instalación local desde cero (Windows y PowerShell)

### 6.1. Requisitos

- **Git** instalado.
- **Docker Desktop** iniciado, con Docker Compose V2 disponible.
- **PowerShell** (los ejemplos siguientes usan su sintaxis).
- Conexión a Internet para descargar imágenes Docker y consultar la API de ARESEP.
- Recursos suficientes para ejecutar múltiples contenedores de Airflow. Se recomienda disponer de al menos 4 GB de memoria para Docker, además de los recursos que necesita el sistema operativo.

No es necesario instalar PostgreSQL ni Airflow directamente en Windows.

### 6.2. Clonar el repositorio

```powershell
git clone https://github.com/KevinVM98/costa-rica-electricity-data-pipeline.git
cd costa-rica-electricity-data-pipeline
```

### 6.3. Configurar PostgreSQL de ARESEP

Crear el archivo local de variables de entorno:

```powershell
Copy-Item .env.example .env
```

Editar `.env` y **reemplazar la contraseña de ejemplo**:

```dotenv
POSTGRES_USER=ice_user
POSTGRES_PASSWORD=ESTABLECER_UNA_CONTRASENA_PROPIA
POSTGRES_DB=ice_data
POSTGRES_PORT=5433
```

El puerto `5433` es el valor predeterminado de esta instalación. Si está ocupado, utilizar otro puerto disponible y actualizar también `ICE_PG_PORT` en `airflow/.env`.

Iniciar PostgreSQL desde la raíz del proyecto:

```powershell
docker compose up -d db
```

En una base nueva, Docker crea un volumen persistente y PostgreSQL ejecuta automáticamente `sql/init.sql`, que prepara los esquemas y tablas del pipeline.

Comprobar el contenedor:

```powershell
docker compose ps db
```

Verificar las tablas del proyecto:

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('raw', 'staging', 'monitoring') AND table_type = 'BASE TABLE' ORDER BY table_schema, table_name;"
```

Se esperan **seis tablas** entre los esquemas `raw`, `staging` y `monitoring`.

Verificar que la tabla RAW está inicialmente vacía:

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT COUNT(*) FROM raw.generacion_planta_v2;"
```

**Nota:** `sql/init.sql` se ejecuta automáticamente al inicializar un **volumen de datos nuevo**. Reiniciar un contenedor con un volumen existente no vuelve a ejecutar ese script.

### 6.4. Preparar Apache Airflow

Crear el archivo de configuración local de Airflow:

```powershell
Copy-Item .\airflow\.env.example .\airflow\.env
```

En `airflow/.env`, revisar especialmente:

```dotenv
AIRFLOW_UID=50000
FERNET_KEY=REEMPLAZAR_CON_CLAVE_FERNET_VALIDA
ICE_PG_HOST=host.docker.internal
ICE_PG_PORT=5433
ICE_PG_DB=ice_data
ICE_PG_USER=ice_user
ICE_PG_PASSWORD=LA_MISMA_CONTRASENA_DE_POSTGRES_PASSWORD
```

- `ICE_PG_PASSWORD` debe coincidir con `POSTGRES_PASSWORD` del `.env` de la raíz.
- `ICE_PG_PORT` debe coincidir con el puerto publicado por PostgreSQL de ARESEP.
- `ICE_PG_HOST=host.docker.internal` permite llegar desde los contenedores de Airflow al puerto publicado por Docker Desktop en Windows.
- `FERNET_KEY` debe contener una clave válida generada localmente; nunca debe subirse a GitHub.

Desde la raíz del repositorio, construir primero la imagen personalizada:

```powershell
Push-Location .\airflow
docker compose -f docker-compose.yaml -f compose.override.yaml build
Pop-Location
```

Generar una clave Fernet válida usando esa imagen:

```powershell
docker run --rm aresep-airflow:3.3.2-dbt python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copiar el valor generado en `FERNET_KEY` dentro de `airflow/.env`, sin comillas ni espacios adicionales. **No publicar ni compartir la clave.**

La configuración Docker Compose de Airflow incluye una cuenta inicial para uso local. Antes de desplegar un entorno compartido, cambiar sus credenciales predeterminadas y revisar la configuración de seguridad.

### 6.5. Inicializar y levantar Airflow

Desde la raíz del repositorio, ingresar a su carpeta:

```powershell
Push-Location .\airflow
```

Inicializar la base de metadatos de Airflow:

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml up -d airflow-init
```

Verificar el resultado de la inicialización:

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml ps -a
```

El servicio `airflow-init` debe finalizar satisfactoriamente (código de salida `0`). Para revisar problemas, consultar:

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml logs airflow-init
```

Iniciar los servicios principales:

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer
```

Revisar su estado:

```powershell
docker compose -f docker-compose.yaml -f compose.override.yaml ps
```

Regresar a la raíz:

```powershell
Pop-Location
```

Abrir la interfaz de Airflow:

**http://localhost:8080**

El puerto web `8080` es el predeterminado en `airflow/docker-compose.yaml`. Si ya está ocupado, cambiar **solo el puerto publicado del API Server**, por ejemplo de `8080:8080` a `8081:8080`, y abrir `http://localhost:8081`.

El primer arranque de Airflow puede requerir más tiempo que los siguientes. Comprobar los estados y logs si algún servicio no alcanza un estado saludable.

## 7. Ejecutar el pipeline

El DAG se llama:

```text
generacion_electrica_aresep
```

Se compone de cinco tareas secuenciales:

1. `registrar_inicio`: crea el registro inicial de la ejecución.
2. `extraer_generacion`: descarga los JSON por año desde ARESEP.
3. `validar_datos`: revisa la estructura, los campos obligatorios, el año, el mes y los campos numéricos de energía.
4. `cargar_raw`: incorpora contenidos nuevos a RAW y registra las métricas de carga.
5. `transformar_datos`: actualiza los objetos SQL de compatibilidad y ejecuta `dbt build` para construir los modelos y probar los datos.

### 7.1. Modos de ejecución

| Parámetro `modo` | Comportamiento |
| --- | --- |
| `mensual` (predeterminado) | Consulta el año actual y el anterior |
| `historico` | Consulta desde 2018 hasta el año actual |

Para ejecutar el histórico desde la interfaz de Airflow:

1. Abrir **Dags** y seleccionar `generacion_electrica_aresep`.
2. Seleccionar **Trigger** y mantener **Single Run**.
3. En **Tipo de carga**, elegir **Histórica (desde 2018)**.
4. Confirmar la ejecución y revisar que las cinco tareas finalicen correctamente.

**No utilizar la función de backfill de Airflow para esta operación:** el modo `historico` ya realiza la descarga de todos los años dentro de **una sola ejecución del DAG**.

Para una ejecución manual de prueba se puede mantener el DAG pausado para evitar activar su programación periódica.

### 7.2. Programación automática

El DAG utiliza la expresión cron `0 6 10 * *`: **día 10 de cada mes a las 6:00 a. m.**, con zona horaria **America/Costa_Rica**. La programación solo opera mientras Airflow esté disponible y el DAG se encuentre habilitado.

## 8. Modelo de datos

### RAW — `raw.generacion_planta_v2`

Conserva cada registro recibido en una columna `JSONB`, junto con atributos de identificación y un hash **SHA-256** calculado a partir de su contenido.

La carga deduplica cada lote y utiliza `ON CONFLICT (contenido_hash) DO NOTHING` para no insertar nuevamente documentos idénticos. También actualiza la fecha de última observación de los contenidos ya conocidos.

Esta estrategia evita duplicados **idénticos por contenido**; no implica que todos los registros de una misma planta y período sean equivalentes.

### STAGING — `staging.generacion_planta`

Extrae los atributos de interés, normaliza textos y convierte los valores de energía a tipos numéricos mediante SQL. En la implementación actual, STAGING se **reconstruye completamente** a partir de RAW al ejecutar las transformaciones.

### MART — `mart.generacion_por_planta`

Es una **vista SQL**, no una tabla de agregados ni una vista materializada. Expone los registros de STAGING, calcula `energia_total` cuando el período tiene un único registro y clasifica las combinaciones ambiguas mediante `estado_periodo`.

Cuando se detectan varios registros para una misma combinación de empresa, planta, año y mes, la vista evita sumar automáticamente esos valores y marca el período como `MULTIPLES_REGISTROS`.

### Modelado analítico con dbt

La tarea `transformar_datos` ejecuta el [proyecto de dbt](dbt/README.md) después de cargar `raw.generacion_planta_v2`. dbt tipa las mediciones en `dbt_dev_staging` y publica dos tablas analíticas en `dbt_dev_mart`. Los objetos de los esquemas `staging` y `mart` mantienen la compatibilidad con las consultas previas.

## 9. Calidad y monitoreo

Antes de cargar RAW, el DAG comprueba:

- Que el conjunto de datos tenga una estructura válida.
- Que existan los campos obligatorios (`anho`, `id_Mes`, `empresaGenera`, `planta`).
- Que el mes esté entre 1 y 12 y que el año corresponda al período consultado.
- Que los campos de energía informados sean numéricos finitos.

Si se detectan errores, la tarea de validación falla y no continúa con la carga correspondiente.

Las métricas se almacenan en:

| Tabla | Contenido |
| --- | --- |
| `monitoring.ejecuciones_pipeline` | Inicio, fin, estado y detalle de fallos |
| `monitoring.calidad_datos` | Registros revisados y errores por año |
| `monitoring.carga_datos` | Registros recibidos, únicos, nuevos, existentes y duración |

## 10. Consultas de verificación

Ejecutar estas consultas desde la **raíz** del proyecto, con el contenedor PostgreSQL iniciado.

**Cobertura histórica y total en RAW:**

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT COUNT(*) AS total_registros, MIN(anho) AS primer_anho, MAX(anho) AS ultimo_anho, COUNT(DISTINCT anho) AS anhos_procesados FROM raw.generacion_planta_v2;"
```

**Registros por año:**

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT anho, COUNT(*) AS registros FROM raw.generacion_planta_v2 GROUP BY anho ORDER BY anho;"
```

**Capas de transformación:**

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT 'staging' AS capa, COUNT(*) AS filas FROM staging.generacion_planta UNION ALL SELECT 'mart' AS capa, COUNT(*) AS filas FROM mart.generacion_por_planta;"
```

**Validaciones de calidad:**

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT anho, registros_revisados, errores_detectados FROM monitoring.calidad_datos ORDER BY anho;"
```

**Resumen de calidad:**

```powershell
docker compose exec db psql -U ice_user -d ice_data -c "SELECT COUNT(DISTINCT anho) AS anhos_validados, SUM(registros_revisados) AS total_revisados, SUM(errores_detectados) AS total_errores FROM monitoring.calidad_datos;"
```

## 11. Prueba de reproducibilidad realizada

Se creó un **clon limpio del repositorio publicado en GitHub**, con una instalación de PostgreSQL y Airflow independiente del entorno de desarrollo habitual.

La prueba comprobó:

- Clonación del código publicado.
- Preparación de archivos de entorno a partir de `.env.example`.
- Creación de la red, el volumen y el contenedor de PostgreSQL.
- Inicialización automática de los esquemas y las tablas mediante `sql/init.sql`.
- Construcción e inicio de Airflow con su imagen personalizada.
- Reconocimiento del DAG sin errores de importación.
- Conexión efectiva del worker de Airflow a la nueva base de datos.
- Ejecución manual del pipeline en modo `historico`.
- Disponibilidad de los registros en RAW, STAGING y MART.

### Resultados observados

| Métrica | Resultado |
| --- | ---: |
| Período de datos | 2018–2026 |
| Años procesados | 9 |
| Registros recibidos de la API durante las pruebas | 5.829 |
| Contenidos únicos almacenados en RAW | 5.819 |
| Registros en STAGING | 5.819 |
| Filas consultables en MART | 5.819 |
| Tareas exitosas de Airflow | 5 de 5 |

En la revisión de calidad realizada, los años consultados aparecieron con **0 errores detectados**. Estos valores describen la ejecución de prueba y pueden cambiar si ARESEP actualiza su fuente.

La reproducibilidad comprobada corresponde a un **entorno local con Docker Desktop**. No equivale todavía a una certificación de despliegue en producción o en múltiples sistemas operativos.

### Ejecutar dos copias en una misma computadora

Para una instalación de prueba **en paralelo** con otra ya levantada, se deben separar:

- El `container_name` de PostgreSQL en el `compose.yaml` del clon.
- El `POSTGRES_PORT` de la raíz y el `ICE_PG_PORT` de Airflow.
- El puerto publicado del `airflow-apiserver` (por ejemplo, `8081:8080`).
- Los nombres de proyecto de Docker Compose, mediante `-p`, para obtener redes y volúmenes independientes.

Durante la prueba se utilizaron los proyectos Compose `aresep-prueba` y `aresep-airflow-prueba`, el contenedor `ice-postgres-prueba` y los puertos `5434` (PostgreSQL) y `8081` (Airflow). **Estos ajustes solo se necesitan cuando ambas instalaciones conviven**; la instalación predeterminada usa los puertos `5433` y `8080`.

## 12. Operación y precauciones

Para detener los servicios de Airflow conservando sus volúmenes:

```powershell
Push-Location .\airflow
docker compose -f docker-compose.yaml -f compose.override.yaml down
Pop-Location
```

Para detener PostgreSQL de ARESEP conservando su volumen:

```powershell
docker compose down
```

**No añadir `-v` a `docker compose down` si se desea conservar los datos.** La eliminación de volúmenes puede borrar las bases persistentes.

Consideraciones de seguridad:

- No publicar `.env`, contraseñas ni claves Fernet.
- Sustituir las credenciales de ejemplo antes de usar el entorno.
- La configuración de Airflow incluida utiliza valores adecuados para pruebas locales; requiere endurecimiento de seguridad antes de exponerse a redes públicas.
- Verificar los datos y las métricas antes de utilizarlos para conclusiones analíticas.

## 13. Estado del proyecto y próximos pasos

### Implementado y comprobado

- Extracción mediante la API REST de ARESEP.
- Carga histórica y carga incremental por contenido.
- Controles de calidad y monitoreo en PostgreSQL.
- Organización de datos en RAW, STAGING y MART.
- Orquestación con Airflow y ejecución del DAG de cinco tareas.
- Inicialización automática de PostgreSQL.
- Imagen de Airflow reproducible mediante Dockerfile.
- Instalación y ejecución histórica desde una clonación limpia de GitHub.
- Pruebas con **pytest** y ejecución en **GitHub Actions**.
- Construcción de modelos analíticos y ejecución de pruebas de datos con **dbt** desde Airflow.

### Mejoras previstas

- Validaciones y modelado analítico más avanzados.
- Almacenamiento tipo Data Lake con archivos **Parquet** y tecnologías como **MinIO**.
- Visualización con **Power BI**.
- Exploración de **PySpark**, **Databricks** e infraestructura en la nube, cuando corresponda.

Estas mejoras forman parte de la hoja de ruta; **todavía no se presentan como funcionalidades implementadas**.



## Pruebas automatizadas e integración continua

El proyecto incorpora pruebas unitarias y de integración con
pytest, ejecutadas automáticamente mediante GitHub Actions.

El objetivo es detectar errores antes de incorporar cambios
a la rama principal del repositorio.

### Cobertura de pruebas

| Componente | Archivo | Casos |
|---|---|---:|
| Selección de períodos | test_periodos.py | 3 |
| Calidad de datos | test_calidad.py | 13 |
| Integración con PostgreSQL | test_postgres_integration.py | 4 |
| Transformaciones SQL | test_transformaciones.py | 4 |
| **Total** | | **24** |

### Pruebas unitarias

Las pruebas unitarias verifican:

- Selección de años en modo mensual e histórico.
- Manejo de modos de ejecución inválidos.
- Presencia de campos obligatorios.
- Validación de meses y años.
- Detección de valores de energía no numéricos.
- Validación de la estructura de los registros.

Estas pruebas no requieren Apache Airflow ni PostgreSQL.

Para ejecutarlas localmente:

```powershell
python -m pytest tests/test_periodos.py tests/test_calidad.py -v
```

### Pruebas de integración con PostgreSQL

Las pruebas de integración utilizan una base de datos
independiente denominada `ice_integration_tests`.

Se verifican los siguientes comportamientos:

- Prevención de duplicados mediante SHA-256.
- Idempotencia al repetir cargas.
- Identificación de registros nuevos y existentes.
- Manejo de cambios en el contenido de los registros.
- Conversión de valores numéricos en STAGING.
- Cálculo de energía total en MART.
- Identificación de períodos ambiguos.
- Reejecución de transformaciones sin duplicaciones.

Las pruebas utilizan transacciones que se revierten
automáticamente para evitar conservar datos ficticios.

Para ejecutarlas localmente, se necesita una instancia
PostgreSQL con el esquema creado mediante `sql/init.sql`.

Variables de entorno necesarias:

- `TEST_PG_HOST`
- `TEST_PG_PORT`
- `TEST_PG_DB`
- `TEST_PG_USER`
- `TEST_PG_PASSWORD`

La base de datos debe llamarse `ice_integration_tests`.

Una vez configurada la conexión, ejecutar:

```powershell
python -m pytest tests/ -v
```

Sin las variables de conexión, las pruebas de integración
se omiten automáticamente.

### Integración continua con GitHub Actions

El proyecto incluye el workflow:

`.github/workflows/tests.yml`

Este workflow se ejecuta automáticamente en los eventos
de push y pull request configurados.

Durante cada ejecución, GitHub Actions:

1. Prepara un entorno Linux con Python.
2. Inicia un contenedor temporal con PostgreSQL 17.
3. Inicializa la base mediante `sql/init.sql`.
4. Instala pytest y psycopg.
5. Ejecuta las pruebas unitarias y de integración.
6. Reporta los resultados de las verificaciones.

La infraestructura temporal de CI no utiliza las bases
de datos locales ni las credenciales del entorno original.

### Protección de la rama principal

El repositorio cuenta con una regla de protección
configurada para `main`.

La configuración requiere:

- Incorporar cambios mediante Pull Requests.
- Superar el check `Run pytest` de GitHub Actions.
- Mantener las ramas actualizadas antes del merge.
- Bloquear eliminaciones y force pushes sobre `main`.

Estas reglas buscan evitar que cambios sin validar
se incorporen al código principal.

### Resultados verificados

- 24 pruebas aprobadas en el entorno local.
- Ejecuciones exitosas de GitHub Actions.
- PostgreSQL temporal utilizado en CI.
- Validación automática de duplicados, cargas incrementales
  y transformaciones SQL.
- Pruebas aisladas de los datos históricos originales.

La batería cubre los escenarios implementados,
pero no representa una validación exhaustiva de todos
los posibles errores del pipeline.









## 14. Fuente y autoría

**Fuente pública:** [ARESEP — Datos Abiertos](https://datos.aresep.go.cr/).

Los datos son propiedad de sus respectivos proveedores. Este repositorio se desarrolla con fines educativos, de aprendizaje y de demostración técnica de prácticas de ingeniería de datos.

**Repositorio:** https://github.com/KevinVM98/costa-rica-electricity-data-pipeline

