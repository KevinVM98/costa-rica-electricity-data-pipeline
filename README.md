
# Costa Rica Electricity Generation Data Pipeline

Pipeline de ingeniería de datos para la extracción,
almacenamiento, transformación y monitoreo de datos
de generación eléctrica por planta en Costa Rica.

## Descripción

Este proyecto implementa una plataforma de datos
local utilizando información pública de la
Autoridad Reguladora de los Servicios Públicos
(ARESEP).

El objetivo es construir un pipeline ELT que permita
procesar información histórica de generación
eléctrica, mantener registros incrementales y
preparar datos para su análisis.

El conjunto de datos utilizado comprende registros
disponibles entre 2018 y 2026.

## Tecnologías utilizadas

- Python: extracción, validación y carga de datos.
- PostgreSQL 17: almacenamiento y consultas SQL.
- Apache Airflow 3: orquestación de pipelines.
- Docker y Docker Compose: infraestructura local.
- SQL: transformaciones y modelado analítico.
- Git y GitHub: control de versiones.

## Arquitectura

```text
              API REST ARESEP
                     |
                     v
              Apache Airflow
                     |
                     v
             Extraer generación
                     |
                     v
              Validar datos
                     |
                     v
              PostgreSQL RAW
             (UPSERT / SHA-256)
                     |
                     v
               STAGING
                     |
                     v
                 MART
                     |
                     v
             Consumo analítico
                (Previsto)

Monitoreo en PostgreSQL:
- Calidad de datos
- Métricas de carga
- Historial de ejecuciones
```

## Orquestación con Apache Airflow

El pipeline se organiza en cinco tareas:

1. registrar_inicio
2. extraer_generacion
3. validar_datos
4. cargar_raw
5. transformar_datos

Las tareas se ejecutan de manera secuencial.

El DAG está configurado para ejecutarse el día
10 de cada mes a las 6:00 a. m., hora de Costa Rica.

La ejecución programada requiere que Docker
y los servicios de Airflow estén disponibles.

## Almacenamiento

PostgreSQL organiza los datos mediante esquemas.

### RAW

Conserva los registros obtenidos de ARESEP
utilizando JSONB.

La tabla raw.generacion_planta_v2 utiliza hashes
SHA-256 para identificar contenidos repetidos.

### STAGING

Organiza las columnas y convierte mediciones
a tipos numéricos mediante SQL.

### MART

Contiene vistas preparadas para consultas analíticas,
incluyendo generación por planta y período.

Los períodos con múltiples registros diferentes
se identifican para evitar agregaciones ambiguas.

## Carga incremental

La ingesta consulta el año actual y el anterior.

Los registros se comparan mediante una huella
SHA-256 calculada sobre su contenido JSON.

- Los contenidos nuevos se insertan.
- Los contenidos existentes no se duplican.
- Se conserva el histórico almacenado.

La transformación STAGING todavía utiliza
una reconstrucción completa.

## Calidad de datos

El pipeline incorpora validaciones de:

- Estructura de los registros.
- Campos obligatorios.
- Año y mes.
- Tipos numéricos de energía.

Si las validaciones fallan, se bloquea
la carga correspondiente.

## Monitoreo

Se utilizan tres tablas:

monitoring.calidad_datos

Registra el número de registros revisados
y los problemas detectados.

monitoring.carga_datos

Registra inserciones nuevas, contenidos existentes
y tiempos de procesamiento.

monitoring.ejecuciones_pipeline

Registra el inicio, finalización y estado
de las ejecuciones del pipeline.

## Resultados del desarrollo

En las pruebas realizadas:

- Se descargaron 5.829 registros históricos.
- Se identificaron 5.819 contenidos únicos.
- Se implementó una carga idempotente por hash.
- Se ejecutó correctamente el DAG de Airflow.
- Se registraron métricas de calidad y carga
  en PostgreSQL.

Los resultados corresponden a los datos
disponibles durante las pruebas.

## Configuración

El proyecto utiliza archivos .env para
las variables de entorno.

Los archivos .env.example contienen referencias
para configurar PostgreSQL y Airflow.

Las credenciales reales y los datos descargados
no se incluyen en el repositorio.

La infraestructura se ejecuta localmente
utilizando Docker Compose.

## Estado del proyecto

Implementado:

- Extracción desde la API de ARESEP.
- PostgreSQL y almacenamiento por capas.
- UPSERT por contenido.
- Orquestación mediante Airflow.
- Validación de calidad de datos.
- Registro de métricas operativas.

Pendiente:

- Automatizar la inicialización del esquema SQL
  para instalaciones nuevas.
- Completar las instrucciones de instalación
  reproducible desde cero.
- Construir dashboards en Power BI.

## Fuente de datos

ARESEP - Datos Abiertos

https://datos.aresep.go.cr/

Los datos pertenecen a sus respectivos
proveedores y se utilizan con fines
educativos y de desarrollo técnico.

## Autoría

Proyecto personal de ingeniería de datos,
desarrollado con fines de aprendizaje
y demostración técnica.

No representa una plataforma oficial
del ICE ni de ARESEP.
