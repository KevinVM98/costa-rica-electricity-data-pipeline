
import ast
import os
from pathlib import Path
from uuid import uuid4

import pytest


# Las pruebas de integración solo se ejecutan cuando
# configuramos expresamente una base de datos de prueba.
if "TEST_PG_DB" not in os.environ:
    pytest.skip(
        "PostgreSQL de integración no configurado",
        allow_module_level=True,
    )

if os.environ["TEST_PG_DB"] != "ice_integration_tests":
    raise RuntimeError(
        "Por seguridad, solo se permite ice_integration_tests"
    )

import psycopg
from psycopg.types.json import Jsonb


DAG_PATH = (
    Path(__file__).resolve().parents[1]
    / "airflow"
    / "dags"
    / "generacion_aresep.py"
)


def obtener_sql_real():
    """Extrae la consulta SQL real del DAG sin importar Airflow."""

    arbol = ast.parse(DAG_PATH.read_text(encoding="utf-8"))

    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "cargar_raw":
            for instruccion in nodo.body:
                if isinstance(instruccion, ast.Assign):
                    if any(
                        isinstance(variable, ast.Name)
                        and variable.id == "sql_insertar"
                        for variable in instruccion.targets
                    ):
                        return ast.literal_eval(instruccion.value)

    raise RuntimeError("No se encontró sql_insertar en el DAG")


@pytest.fixture
def conexion():
    """Cada prueba se revierte automáticamente."""

    conn = psycopg.connect(
        host=os.getenv("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.getenv("TEST_PG_PORT", "5434")),
        dbname=os.environ["TEST_PG_DB"],
        user=os.getenv("TEST_PG_USER", "ice_user"),
        password=os.environ["TEST_PG_PASSWORD"],
        connect_timeout=10,
    )

    try:
        nombre = conn.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        assert nombre == "ice_integration_tests"

        yield conn
    finally:
        conn.rollback()
        conn.close()


def crear_registro(planta):
    return {
        "anho": 2026,
        "id_Mes": 1,
        "empresaGenera": "ICE",
        "planta": planta,
        "energiaPunta": "10",
        "energiaValle": "5",
        "energiaNoche": "2",
    }


def insertar_lote(conexion, registros):
    sql = obtener_sql_real()

    with conexion.cursor() as cursor:
        cursor.execute(sql, (Jsonb(registros),))
        return cursor.fetchone()


def contar_planta(conexion, planta):
    return conexion.execute(
        """
        SELECT COUNT(*)
        FROM raw.generacion_planta_v2
        WHERE planta = %s
        """,
        (planta,),
    ).fetchone()[0]


def test_duplicados_en_mismo_lote(conexion):
    planta = f"TEST_{uuid4().hex}"
    registro = crear_registro(planta)

    unicos, nuevos = insertar_lote(
        conexion, [registro, registro]
    )

    assert unicos == 1
    assert nuevos == 1
    assert contar_planta(conexion, planta) == 1


def test_carga_idempotente(conexion):
    planta = f"TEST_{uuid4().hex}"
    registro = crear_registro(planta)

    primera = insertar_lote(conexion, [registro])
    segunda = insertar_lote(conexion, [registro])

    assert primera == (1, 1)
    assert segunda == (1, 0)
    assert contar_planta(conexion, planta) == 1


def test_carga_incremental(conexion):
    planta_a = f"TEST_{uuid4().hex}"
    planta_b = f"TEST_{uuid4().hex}"

    registro_a = crear_registro(planta_a)
    registro_b = crear_registro(planta_b)

    insertar_lote(conexion, [registro_a])

    unicos, nuevos = insertar_lote(
        conexion, [registro_a, registro_b]
    )

    existentes = unicos - nuevos

    assert unicos == 2
    assert nuevos == 1
    assert existentes == 1
    assert contar_planta(conexion, planta_a) == 1
    assert contar_planta(conexion, planta_b) == 1


def test_cambio_contenido_genera_nuevo_hash(conexion):
    planta = f"TEST_{uuid4().hex}"

    original = crear_registro(planta)
    modificado = crear_registro(planta)
    modificado["energiaPunta"] = "11"

    insertar_lote(conexion, [original])
    unicos, nuevos = insertar_lote(conexion, [modificado])

    assert unicos == 1
    assert nuevos == 1
    assert contar_planta(conexion, planta) == 2
