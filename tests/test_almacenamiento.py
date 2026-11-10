"""Pruebas del codigo real con Parquet real y un doble de S3 en memoria."""

import ast
import json
import sys
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


RUTA_DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(RUTA_DAGS))

from almacenamiento import (
    AlmacenamientoS3,
    PREFIJO_PROCESSED,
    construir_tabla,
    esquema_parquet,
)


class S3Memoria:
    def __init__(self):
        self.objetos = {}
        self.escrituras = []
        self.corromper_parquet = False
        self.fallar_anho = None

    def put_object(self, **argumentos):
        clave = argumentos["Key"]
        if self.fallar_anho and f"/anho={self.fallar_anho}/" in clave:
            raise OSError("Fallo de almacenamiento simulado")
        self.objetos[clave] = bytes(argumentos["Body"])
        self.escrituras.append(clave)
        return {}

    def get_object(self, **argumentos):
        clave = argumentos["Key"]
        contenido = self.objetos[clave]
        if self.corromper_parquet and clave.endswith(".parquet"):
            contenido = b"archivo alterado"
        return {"Body": BytesIO(contenido)}


@pytest.fixture
def s3():
    cliente = S3Memoria()
    return AlmacenamientoS3(cliente=cliente, bucket="aresep-test"), cliente


@pytest.fixture
def fila():
    return {
        "raw_id": 1,
        "anho": 2025,
        "id_mes": 9,
        "empresa_genera": "Empresa de prueba",
        "planta": "Planta de prueba",
        "energia_punta": Decimal("12345678901234567890.123456789012"),
        "energia_valle": None,
        "energia_noche": Decimal("0"),
        "factor_carga": Decimal("0.75"),
        "contenido_hash": "a" * 64,
    }


def test_raw_conserva_bytes_originales_y_metadata(s3):
    almacenamiento, cliente = s3
    contenido = '{ "metadata": {"nombre": "generación"}, "value": [] }'.encode()
    ref = almacenamiento.guardar_raw("dag", "run", 2025, contenido)
    assert cliente.objetos[ref["key"]] == contenido
    assert almacenamiento.leer_json(ref)["metadata"]["nombre"] == "generación"


def test_raw_reintento_reutiliza_clave_pero_nueva_respuesta_se_conserva(s3):
    almacenamiento, cliente = s3
    primero = almacenamiento.guardar_raw("dag", "run", 2025, b'{"value": [1]}')
    repetido = almacenamiento.guardar_raw("dag", "run", 2025, b'{"value": [1]}')
    cambiado = almacenamiento.guardar_raw("dag", "run", 2025, b'{"value": [2]}')
    otra_ejecucion = almacenamiento.guardar_raw("dag", "run2", 2025, b'{"value": [1]}')
    assert primero == repetido
    assert len(cliente.objetos) == 3
    assert len({primero["key"], cambiado["key"], otra_ejecucion["key"]}) == 3


def test_raw_corrupto_falla_incluso_con_igual_longitud(s3):
    almacenamiento, cliente = s3
    ref = almacenamiento.guardar_raw("dag", "run", 2025, b'{"value": [1]}')
    cliente.objetos[ref["key"]] = b'{"value": [2]}'
    with pytest.raises(ValueError, match="SHA-256"):
        almacenamiento.leer_json(ref)


def test_no_acepta_referencia_de_otro_bucket(s3):
    almacenamiento, _ = s3
    ref = almacenamiento.guardar_raw("dag", "run", 2025, b'{}')
    with pytest.raises(ValueError, match="bucket"):
        almacenamiento.leer_bytes({**ref, "bucket": "otro-bucket"})


def test_parquet_conserva_decimales_nulos_y_ceros(fila):
    tabla = construir_tabla([fila])
    buffer = pa.BufferOutputStream()
    pq.write_table(tabla, buffer, compression="snappy")
    recuperada = pq.read_table(pa.BufferReader(buffer.getvalue()))
    assert recuperada.schema.equals(esquema_parquet(), check_metadata=True)
    assert recuperada.to_pylist() == [fila]


@pytest.mark.parametrize("campo", ["anho", "id_mes", "planta", "contenido_hash"])
def test_rechaza_nulos_obligatorios(fila, campo):
    fila[campo] = None
    with pytest.raises(ValueError, match="no admite nulos"):
        construir_tabla([fila])


@pytest.mark.parametrize("mes", [0, 13, 1.5, True])
def test_rechaza_meses_invalidos(fila, mes):
    fila["id_mes"] = mes
    with pytest.raises(ValueError, match="id_mes"):
        construir_tabla([fila])


@pytest.mark.parametrize("campo", ["raw_id", "contenido_hash"])
def test_rechaza_duplicados_del_grano_raw(fila, campo):
    otra = {**fila, "raw_id": 2, "contenido_hash": "b" * 64}
    otra[campo] = fila[campo]
    with pytest.raises(ValueError, match="duplicado"):
        construir_tabla([fila, otra])


@pytest.mark.parametrize("cambio", ["agregar", "quitar"])
def test_detecta_cambios_de_columnas(fila, cambio):
    if cambio == "agregar":
        fila["campo_nuevo"] = "valor"
    else:
        fila.pop("energia_valle")
    with pytest.raises(ValueError, match="Esquema STAGING incompatible"):
        construir_tabla([fila])


@pytest.mark.parametrize("valor", [Decimal("NaN"), Decimal("Infinity"), 0.1])
def test_rechaza_decimales_no_exactos_o_no_finitos(fila, valor):
    fila["energia_punta"] = valor
    with pytest.raises(ValueError, match="Decimal finito"):
        construir_tabla([fila])


@pytest.mark.parametrize("valor", [Decimal("0.1234567890123"), Decimal("1e26")])
def test_exceso_de_precision_falla_sin_redondear(fila, valor):
    fila["energia_punta"] = valor
    with pytest.raises(pa.ArrowInvalid):
        construir_tabla([fila])


def test_snapshot_particionado_y_manifiesto_publicado_al_final(s3, fila):
    almacenamiento, cliente = s3
    otra = {**fila, "anho": 2026, "raw_id": 2, "contenido_hash": "b" * 64}
    resultado = almacenamiento.publicar_snapshot([otra, fila], "dag", "run")
    latest = f"{PREFIJO_PROCESSED}/latest.json"
    manifiesto = json.loads(cliente.objetos[latest])
    assert resultado["filas"] == manifiesto["filas_total"] == 2
    assert [a["anho"] for a in manifiesto["archivos"]] == [2025, 2026]
    assert sum(a["filas"] for a in manifiesto["archivos"]) == 2
    assert cliente.escrituras[-2:] == [resultado["manifest"]["key"], latest]
    for archivo in manifiesto["archivos"]:
        tabla = pq.read_table(pa.BufferReader(almacenamiento.leer_bytes(archivo)))
        assert tabla.num_rows == archivo["filas"]
        assert tabla.column("anho").to_pylist() == [archivo["anho"]]


def test_reintentar_snapshot_no_duplica_archivos_y_cambio_conserva_version(s3, fila):
    almacenamiento, cliente = s3
    primero = almacenamiento.publicar_snapshot([fila], "dag", "run")
    repetido = almacenamiento.publicar_snapshot([fila], "dag", "run")
    assert primero["archivos"] == repetido["archivos"]
    assert len(cliente.objetos) == 3  # un Parquet, manifest y latest
    cambiado = almacenamiento.publicar_snapshot(
        [{**fila, "energia_noche": Decimal("8")}], "dag", "run"
    )
    assert cambiado["archivos"][0]["key"] != primero["archivos"][0]["key"]
    assert primero["archivos"][0]["key"] in cliente.objetos
    latest = json.loads(cliente.objetos[f"{PREFIJO_PROCESSED}/latest.json"])
    assert latest["archivos"] == cambiado["archivos"]


@pytest.mark.parametrize("problema", ["escritura", "corrupcion", "vacio"])
def test_fallo_no_reemplaza_ultimo_snapshot_valido(s3, fila, problema):
    almacenamiento, cliente = s3
    almacenamiento.publicar_snapshot([fila], "dag", "anterior")
    latest = f"{PREFIJO_PROCESSED}/latest.json"
    anterior = cliente.objetos[latest]
    registros = [fila, {**fila, "anho": 2026, "raw_id": 2, "contenido_hash": "b" * 64}]
    escrituras_antes = len(cliente.escrituras)
    if problema == "escritura":
        cliente.fallar_anho = 2026
    elif problema == "corrupcion":
        cliente.corromper_parquet = True
    else:
        registros = []

    with pytest.raises((ValueError, OSError)):
        almacenamiento.publicar_snapshot(registros, "dag", "nuevo")

    assert cliente.objetos[latest] == anterior
    assert not any(k.endswith("manifest.json") for k in cliente.escrituras[escrituras_antes:])


def extraer_funcion_del_dag(nombre, entorno):
    """Ejecuta la funcion real del DAG sin instalar ni simular Airflow."""
    arbol = ast.parse((RUTA_DAGS / "generacion_aresep.py").read_text(encoding="utf-8"))
    funcion = next(n for n in ast.walk(arbol) if isinstance(n, ast.FunctionDef) and n.name == nombre)
    funcion.decorator_list = []
    modulo = ast.Module(body=[funcion], type_ignores=[])
    exec(compile(ast.fix_missing_locations(modulo), "generacion_aresep.py", "exec"), entorno)
    return entorno[nombre]


@pytest.mark.parametrize("respuesta_valida", [True, False])
def test_extraccion_real_archiva_antes_de_validar_y_devuelve_referencias(
    s3, monkeypatch, respuesta_valida
):
    almacenamiento, cliente = s3
    contenido = b'{"metadata": {}, "value": [{"anho": 2025}]}' if respuesta_valida else b'no es JSON'

    class Respuesta:
        content = contenido

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

    class Sesion(Respuesta):
        def get(self, url, timeout):
            return Respuesta()

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(Session=Sesion))
    funcion = extraer_funcion_del_dag("extraer_generacion", {
        "json": json,
        "BASE_URL": "https://example.invalid/api",
        "AlmacenamientoS3": lambda: almacenamiento,
        "obtener_anhos_a_procesar": lambda: [2025],
        "get_current_context": lambda: {
            "ti": SimpleNamespace(dag_id="dag", run_id="run"),
            "params": {"modo": "mensual"},
        },
    })

    if respuesta_valida:
        resultado = funcion()
        assert resultado["lotes"][0]["registros"] == 1
        assert almacenamiento.leer_bytes(resultado["lotes"][0]["objeto"]) == contenido
        assert "value" not in resultado
    else:
        with pytest.raises(json.JSONDecodeError):
            funcion()
        assert list(cliente.objetos.values()) == [contenido]
