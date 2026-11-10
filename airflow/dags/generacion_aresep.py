
import json
import os
import subprocess
import time

from pathlib import Path
from datetime import timedelta
from tempfile import TemporaryDirectory

import pendulum

from airflow.sdk import dag, task, get_current_context, Param
from periodos import seleccionar_anhos
from calidad import validar_registro
from almacenamiento import AlmacenamientoS3


# ==========================================
# CONFIGURACION GENERAL
# ==========================================

BASE_URL = (
    "https://datos.aresep.go.cr/ws.datosabiertos/"
    "Services/IE/Electricidad.svc/"
    "ObtenerGeneracionFuentePlantaYCapacidadInstalada"
)

SQL = Path("/opt/airflow/sql/transformaciones.sql")
DBT_CLI = "/opt/airflow/dbt-venv/bin/dbt"
DBT_PROYECTO = "/opt/airflow/dbt"


def obtener_anhos_a_procesar():
    """Selecciona los anos segun el modo de ejecucion del DAG."""
    contexto = get_current_context()
    modo = contexto["params"]["modo"]
    anho_actual = pendulum.now("America/Costa_Rica").year

    return seleccionar_anhos(modo, anho_actual)


def configurar_postgres():
    return {
        "host": os.environ["ICE_PG_HOST"],
        "port": int(os.environ["ICE_PG_PORT"]),
        "dbname": os.environ["ICE_PG_DB"],
        "user": os.environ["ICE_PG_USER"],
        "password": os.environ["ICE_PG_PASSWORD"],
    }


# ==========================================
# CALLBACKS DE MONITOREO
# ==========================================

def registrar_estado_final(contexto, estado):
    import psycopg

    dag_run_id = contexto["dag_run"].run_id

    tarea_fallida = None
    detalle_error = None

    if estado == "FALLIDO":

        tarea = contexto.get("task_instance")

        if tarea is not None:
            tarea_fallida = tarea.task_id

        excepcion = contexto.get("exception")

        detalle_error = (
            str(excepcion)
            if excepcion is not None
            else "Ejecucion fallida. Revisar logs de Airflow."
        )

    with psycopg.connect(
        **configurar_postgres()
    ) as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO monitoring.ejecuciones_pipeline (
                    dag_run_id,
                    estado,
                    tarea_fallida,
                    detalle_error,
                    fecha_inicio,
                    fecha_fin
                )
                VALUES (
                    %s, %s, %s, %s, NOW(), NOW()
                )

                ON CONFLICT (dag_run_id)
                DO UPDATE SET
                    estado = EXCLUDED.estado,
                    tarea_fallida = EXCLUDED.tarea_fallida,
                    detalle_error = EXCLUDED.detalle_error,
                    fecha_fin = NOW()
                """,
                (
                    dag_run_id,
                    estado,
                    tarea_fallida,
                    detalle_error
                )
            )

    print(
        f"DAG {dag_run_id} finalizado: {estado}"
    )


def callback_exito(contexto):
    registrar_estado_final(
        contexto,
        "EXITOSO"
    )


def callback_fallo(contexto):
    registrar_estado_final(
        contexto,
        "FALLIDO"
    )


# ==========================================
# DEFINICION DEL DAG
# ==========================================

@dag(
    dag_id="generacion_electrica_aresep",

    schedule="0 6 10 * *",

    start_date=pendulum.datetime(
        2026, 11, 1,
        tz="America/Costa_Rica"
    ),

    catchup=False,
    max_active_runs=1,

    # En ejecuciones programadas se usa el modo mensual.
    # Para hacer un backfill, seleccionar "historico" al lanzar
    # manualmente el DAG desde la interfaz de Airflow.
    params={
        "modo": Param(
            "mensual",
            type="string",
            enum=["mensual", "historico"],
            title="Tipo de carga",
            description=(
                "mensual: ano actual y anterior; "
                "historico: todos los anos desde 2018 hasta hoy."
            ),
            values_display={
                "mensual": "Mensual (actual y anterior)",
                "historico": "Historica (desde 2018)",
            },
        )
    },

    on_success_callback=callback_exito,
    on_failure_callback=callback_fallo,

    tags=[
        "ARESEP",
        "Data Engineering",
        "ELT",
        "Monitoring"
    ],
)
def pipeline_generacion():

    # ======================================
    # TAREA 1: REGISTRAR INICIO
    # ======================================

    @task(retries=1)
    def registrar_inicio():
        import psycopg

        contexto = get_current_context()
        dag_run_id = contexto["ti"].run_id

        with psycopg.connect(
            **configurar_postgres()
        ) as conn:

            with conn.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO monitoring.ejecuciones_pipeline (
                        dag_run_id,
                        estado,
                        fecha_inicio
                    )
                    VALUES (%s, 'INICIADO', NOW())

                    ON CONFLICT (dag_run_id)
                    DO UPDATE SET
                        estado = 'INICIADO',
                        fecha_inicio = NOW(),
                        fecha_fin = NULL,
                        tarea_fallida = NULL,
                        detalle_error = NULL
                    """,
                    (dag_run_id,)
                )

        modo = contexto["params"]["modo"]
        print(f"Pipeline iniciado: {dag_run_id} (modo: {modo})")

    # ======================================
    # TAREA 2: EXTRACCION
    # ======================================

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    def extraer_generacion():
        import requests

        contexto = get_current_context()
        dag_id = contexto["ti"].dag_id
        run_id = contexto["ti"].run_id
        almacenamiento = AlmacenamientoS3()
        anhos = obtener_anhos_a_procesar()
        lotes = []
        total = 0

        with requests.Session() as sesion:
            for anho in anhos:
                url = f"{BASE_URL}/{anho}"
                print(f"Descargando {anho}...")

                with sesion.get(url, timeout=120) as respuesta:
                    respuesta.raise_for_status()
                    contenido_original = respuesta.content

                # Guardar el cuerpo original ANTES de validar su estructura.
                # Si cambia la API, queda evidencia para investigar el fallo.
                referencia = almacenamiento.guardar_raw(
                    dag_id, run_id, anho, contenido_original
                )
                contenido = json.loads(contenido_original)
                if not isinstance(contenido, dict):
                    raise ValueError(f"Respuesta JSON inesperada para {anho}")
                registros = contenido.get("value")

                if not isinstance(registros, list) or not registros:
                    raise ValueError(f"Sin registros para {anho}")
                if not all(
                    isinstance(r, dict)
                    and str(r.get("anho")) == str(anho)
                    for r in registros
                ):
                    raise ValueError(f"Registros inesperados para {anho}")

                lotes.append({
                    "anho": anho,
                    "url": url,
                    "registros": len(registros),
                    "objeto": referencia,
                })
                total += len(registros)
                print(
                    f"{anho}: {len(registros)} registros archivados en "
                    f"s3://{referencia['bucket']}/{referencia['key']}"
                )

        manifest = almacenamiento.publicar_extraccion(
            dag_id, run_id, contexto["params"]["modo"], lotes
        )
        print(f"Extraccion completada: {total}")
        # XCom recibe referencias pequenas; los documentos permanecen en MinIO.
        return {"manifest": manifest, "lotes": lotes}

    # ======================================
    # TAREA 3: CALIDAD DE DATOS
    # ======================================

    @task(retries=1)
    def validar_datos(extraccion):
        import psycopg

        contexto = get_current_context()
        dag_run_id = contexto["ti"].run_id

        errores = []
        resumen = []
        total_revisados = 0
        almacenamiento = AlmacenamientoS3()

        for lote in extraccion["lotes"]:
            anho = lote["anho"]
            contenido = almacenamiento.leer_json(lote["objeto"])

            registros = contenido.get("value")

            if not isinstance(registros, list) or not registros:
                raise ValueError(
                    f"Archivo sin registros: {anho}"
                )

            errores_anho = 0

            for numero, registro in enumerate(registros, start=1):
                errores_registro = validar_registro(
                    registro,
                    anho,
                    numero
                )
                errores.extend(errores_registro)
                errores_anho += len(errores_registro)

            total_revisados += len(registros)

            resumen.append(
                (
                    dag_run_id,
                    anho,
                    len(registros),
                    errores_anho
                )
            )

            print(
                f"{anho}: {len(registros)} revisados, "
                f"{errores_anho} problemas"
            )

        with psycopg.connect(
            **configurar_postgres()
        ) as conn:

            with conn.cursor() as cursor:

                cursor.executemany(
                    """
                    INSERT INTO monitoring.calidad_datos (
                        dag_run_id,
                        anho,
                        registros_revisados,
                        errores_detectados
                    )
                    VALUES (%s, %s, %s, %s)

                    ON CONFLICT (dag_run_id, anho)
                    DO UPDATE SET
                        registros_revisados =
                            EXCLUDED.registros_revisados,
                        errores_detectados =
                            EXCLUDED.errores_detectados,
                        fecha_validacion = NOW()
                    """,
                    resumen
                )

        print(
            f"Total revisados: {total_revisados}"
        )
        print(
            f"Problemas detectados: {len(errores)}"
        )

        if errores:
            for error in errores[:10]:
                print("ERROR:", error)

            raise ValueError(
                f"Validacion fallida: {len(errores)} problemas"
            )

        print("Calidad de datos: VALIDACION EXITOSA")

    # ======================================
    # TAREA 4: CARGA RAW INCREMENTAL
    # ======================================

    @task(retries=1)
    def cargar_raw(extraccion):
        import psycopg
        from psycopg.types.json import Jsonb

        contexto = get_current_context()
        dag_run_id = contexto["ti"].run_id
        almacenamiento = AlmacenamientoS3()

        sql_insertar = """
            WITH entrada AS (
                SELECT elemento AS documento
                FROM jsonb_array_elements(
                    %s::jsonb
                ) AS fuente(elemento)
            ),

            normalizados AS (
                SELECT
                    (documento->>'anho')::SMALLINT AS anho,
                    (documento->>'id_Mes')::SMALLINT AS id_mes,
                    documento->>'empresaGenera' AS empresa_genera,
                    documento->>'planta' AS planta,
                    documento AS datos,

                    encode(
                        sha256(
                            convert_to(
                                documento::text,
                                'UTF8'
                            )
                        ),
                        'hex'
                    ) AS contenido_hash
                FROM entrada
            ),

            unicos AS (
                SELECT DISTINCT ON (contenido_hash)
                    anho,
                    id_mes,
                    empresa_genera,
                    planta,
                    datos,
                    contenido_hash
                FROM normalizados
                ORDER BY contenido_hash
            ),

            insertados AS (
                INSERT INTO raw.generacion_planta_v2 (
                    anho,
                    id_mes,
                    empresa_genera,
                    planta,
                    datos,
                    contenido_hash
                )
                SELECT
                    anho,
                    id_mes,
                    empresa_genera,
                    planta,
                    datos,
                    contenido_hash
                FROM unicos
                WHERE TRUE

                ON CONFLICT (contenido_hash)
                DO NOTHING

                RETURNING contenido_hash
            )

            SELECT
                (SELECT COUNT(*) FROM unicos),
                (SELECT COUNT(*) FROM insertados)
        """

        sql_actualizar = """
            UPDATE raw.generacion_planta_v2 AS destino

            SET fecha_ultima_vista = NOW()

            FROM (
                SELECT DISTINCT
                    encode(
                        sha256(
                            convert_to(
                                elemento::text,
                                'UTF8'
                            )
                        ),
                        'hex'
                    ) AS contenido_hash

                FROM jsonb_array_elements(
                    %s::jsonb
                ) AS fuente(elemento)
            ) AS lote

            WHERE destino.contenido_hash =
                  lote.contenido_hash
        """

        resumen = []
        total_nuevos = 0
        total_existentes = 0

        with psycopg.connect(
            **configurar_postgres()
        ) as conn:

            with conn.cursor() as cursor:

                for lote in extraccion["lotes"]:

                    inicio = time.perf_counter()
                    anho = lote["anho"]
                    contenido = almacenamiento.leer_json(lote["objeto"])

                    registros = contenido["value"]

                    if not registros:
                        raise ValueError(
                            f"Sin registros para {anho}"
                        )

                    recibidos = len(registros)

                    cursor.execute(
                        sql_insertar,
                        (Jsonb(registros),)
                    )

                    unicos, nuevos = cursor.fetchone()

                    cursor.execute(
                        sql_actualizar,
                        (Jsonb(registros),)
                    )

                    existentes = unicos - nuevos

                    duracion = round(
                        time.perf_counter() - inicio,
                        3
                    )

                    resumen.append(
                        (
                            dag_run_id,
                            anho,
                            recibidos,
                            unicos,
                            nuevos,
                            existentes,
                            duracion
                        )
                    )

                    total_nuevos += nuevos
                    total_existentes += existentes

                    print(
                        f"{anho}: "
                        f"{recibidos} recibidos, "
                        f"{nuevos} nuevos, "
                        f"{existentes} existentes"
                    )

                cursor.executemany(
                    """
                    INSERT INTO monitoring.carga_datos (
                        dag_run_id,
                        anho,
                        registros_recibidos,
                        registros_unicos_lote,
                        registros_nuevos,
                        registros_existentes,
                        duracion_segundos
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )

                    ON CONFLICT (dag_run_id, anho)
                    DO UPDATE SET
                        registros_recibidos =
                            EXCLUDED.registros_recibidos,
                        registros_unicos_lote =
                            EXCLUDED.registros_unicos_lote,
                        registros_nuevos =
                            EXCLUDED.registros_nuevos,
                        registros_existentes =
                            EXCLUDED.registros_existentes,
                        duracion_segundos =
                            EXCLUDED.duracion_segundos,
                        fecha_carga = NOW()
                    """,
                    resumen
                )

        print(
            f"Carga completada: "
            f"{total_nuevos} nuevos, "
            f"{total_existentes} existentes"
        )

    # ======================================
    # TAREA 5: TRANSFORMACIONES
    # ======================================

    @task(retries=1)
    def transformar_datos():
        import psycopg

        postgres = configurar_postgres()
        contenido_sql = SQL.read_text(
            encoding="utf-8"
        )

        with psycopg.connect(
            **postgres,
            autocommit=True
        ) as conn:

            with conn.cursor() as cursor:
                cursor.execute(contenido_sql)

        print("STAGING y MART SQL actualizados", flush=True)

        entorno_dbt = {
            **os.environ,
            "ARESEP_DBT_PG_HOST": postgres["host"],
            "ARESEP_DBT_PG_PORT": str(postgres["port"]),
            "ARESEP_DBT_PG_DB": postgres["dbname"],
            "ARESEP_DBT_PG_USER": postgres["user"],
            "DBT_ENV_SECRET_PG_PASSWORD": postgres["password"],
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DBT_USE_COLORS": "false",
        }

        with TemporaryDirectory(prefix="aresep-dbt-") as directorio:
            artefactos = Path(directorio)
            subprocess.run(
                [
                    DBT_CLI,
                    "build",
                    "--project-dir", DBT_PROYECTO,
                    "--profiles-dir", DBT_PROYECTO,
                    "--target-path", str(artefactos / "target"),
                    "--log-path", str(artefactos / "logs"),
                ],
                cwd=directorio,
                env=entorno_dbt,
                check=True,
            )

        print("MARTs de dbt actualizados y validados")

    # ======================================
    # TAREA 6: SNAPSHOT PARQUET DEL STAGING DBT
    # ======================================

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def exportar_parquet(extraccion):
        import psycopg
        from psycopg.rows import dict_row

        contexto = get_current_context()
        # Snapshot consistente de la vista. SELECT * permite detectar cambios
        # de columnas al aplicar el contrato de exportacion.
        with psycopg.connect(
            **configurar_postgres(), row_factory=dict_row
        ) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            registros = conn.execute(
                "SELECT * FROM dbt_dev_staging.stg_generacion_planta "
                "ORDER BY anho, contenido_hash"
            ).fetchall()

        resultado = AlmacenamientoS3().publicar_snapshot(
            registros,
            contexto["ti"].dag_id,
            contexto["ti"].run_id,
            extraccion=extraccion["manifest"],
        )
        for archivo in resultado["archivos"]:
            print(
                f"{archivo['anho']}: {archivo['filas']} filas Parquet "
                f"validadas en s3://{archivo['bucket']}/{archivo['key']}"
            )
        print(f"Snapshot publicado: {resultado['filas']} filas")
        print("Pipeline ELT y almacenamiento en MinIO completados")
        return resultado["manifest"]

    # ======================================
    # DEPENDENCIAS
    # ======================================

    inicio = registrar_inicio()
    extraccion = extraer_generacion()
    validacion = validar_datos(extraccion)
    carga = cargar_raw(extraccion)
    transformacion = transformar_datos()
    exportacion = exportar_parquet(extraccion)

    inicio >> extraccion >> validacion >> carga >> transformacion >> exportacion


pipeline_generacion()
