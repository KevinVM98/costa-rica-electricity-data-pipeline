
import hashlib
import json
import os
import re
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

if "TEST_PG_DB" not in os.environ:
    pytest.skip(
        "Base de integración no configurada",
        allow_module_level=True,
    )

if os.environ["TEST_PG_DB"] != "ice_integration_tests":
    raise RuntimeError("Base de pruebas incorrecta")

import psycopg
from psycopg.types.json import Jsonb


SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "transformaciones.sql"
)


@pytest.fixture
def conexion():
    conn = psycopg.connect(
        host=os.getenv("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.getenv("TEST_PG_PORT", "5434")),
        dbname=os.environ["TEST_PG_DB"],
        user=os.getenv("TEST_PG_USER", "ice_user"),
        password=os.environ["TEST_PG_PASSWORD"],
    )

    try:
        assert conn.execute(
            "SELECT current_database()"
        ).fetchone()[0] == "ice_integration_tests"

        yield conn
    finally:
        # Revierte los datos y las transformaciones
        # ejecutadas durante cada prueba.
        conn.rollback()
        conn.close()


def insertar_registro(conn, planta, punta="10.5"):
    datos = {
        "anho": 2026,
        "id_Mes": 1,
        "empresaGenera": "ICE",
        "planta": planta,
        "energiaPunta": punta,
        "energiaValle": "5",
        "energiaNoche": "2",
        "factorCarga": "0.7",
    }

    contenido = json.dumps(datos, sort_keys=True)
    hash_contenido = hashlib.sha256(
        contenido.encode("utf-8")
    ).hexdigest()

    conn.execute(
        """
        INSERT INTO raw.generacion_planta_v2 (
            anho, id_mes, empresa_genera,
            planta, datos, contenido_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            2026, 1, "ICE", planta,
            Jsonb(datos), hash_contenido,
        ),
    )


def ejecutar_transformaciones(conn):
    sql = SQL_PATH.read_text(encoding="utf-8")

    # El SQL productivo contiene BEGIN y COMMIT.
    # Los retiramos únicamente durante las pruebas
    # para permitir que pytest revierta la transacción.
    assert re.match(r"^\s*BEGIN\s*;", sql, re.I)
    assert re.search(r"\bCOMMIT\s*;\s*$", sql, re.I)

    sql = re.sub(
        r"^\s*BEGIN\s*;", "", sql,
        count=1, flags=re.I
    )

    sql = re.sub(
        r"\bCOMMIT\s*;\s*$", "", sql,
        count=1, flags=re.I
    )

    conn.execute(sql)


def test_staging_convierte_numericos(conexion):
    planta = f"TEST_{uuid4().hex}"
    insertar_registro(conexion, planta)

    ejecutar_transformaciones(conexion)

    fila = conexion.execute(
        """
        SELECT energia_punta, energia_valle,
               energia_noche, factor_carga
        FROM staging.generacion_planta
        WHERE planta = %s
        """,
        (planta,),
    ).fetchone()

    assert fila == (
        Decimal("10.5"),
        Decimal("5"),
        Decimal("2"),
        Decimal("0.7"),
    )


def test_mart_calcula_energia_total(conexion):
    planta = f"TEST_{uuid4().hex}"
    insertar_registro(conexion, planta)

    ejecutar_transformaciones(conexion)

    fila = conexion.execute(
        """
        SELECT energia_total, registros_periodo,
               estado_periodo
        FROM mart.generacion_por_planta
        WHERE planta = %s
        """,
        (planta,),
    ).fetchone()

    assert fila == (
        Decimal("17.5"),
        1,
        "UNICO",
    )


def test_mart_identifica_periodos_ambiguos(conexion):
    planta = f"TEST_{uuid4().hex}"

    insertar_registro(conexion, planta, "10")
    insertar_registro(conexion, planta, "20")

    ejecutar_transformaciones(conexion)

    filas = conexion.execute(
        """
        SELECT energia_total, registros_periodo,
               estado_periodo
        FROM mart.generacion_por_planta
        WHERE planta = %s
        """,
        (planta,),
    ).fetchall()

    assert len(filas) == 2

    for energia, cantidad, estado in filas:
        assert energia is None
        assert cantidad == 2
        assert estado == "MULTIPLES_REGISTROS"


def test_transformacion_repetida_no_duplica(conexion):
    planta = f"TEST_{uuid4().hex}"
    insertar_registro(conexion, planta)

    ejecutar_transformaciones(conexion)
    ejecutar_transformaciones(conexion)

    total = conexion.execute(
        """
        SELECT COUNT(*)
        FROM staging.generacion_planta
        WHERE planta = %s
        """,
        (planta,),
    ).fetchone()[0]

    assert total == 1
