
import sys
from pathlib import Path

import pytest


# Importamos el módulo real utilizado por nuestro DAG.
RUTA_DAGS = (
    Path(__file__).resolve().parents[1]
    / "airflow"
    / "dags"
)

sys.path.insert(0, str(RUTA_DAGS))

from calidad import validar_registro


# ==========================================
# DATOS DE PRUEBA
# ==========================================

@pytest.fixture
def registro_valido():
    """Registro ficticio con estructura válida de ARESEP."""
    return {
        "anho": 2026,
        "id_Mes": 9,
        "empresaGenera": "ICE",
        "planta": "Planta A",
        "energiaPunta": "120.5",
        "energiaValle": "80.2",
        "energiaNoche": "45.0",
    }


# ==========================================
# PRUEBA 1: REGISTRO CORRECTO
# ==========================================

def test_registro_valido(registro_valido):

    errores = validar_registro(
        registro_valido, 2026, 1
    )

    assert errores == []


# ==========================================
# PRUEBA 2: CAMPOS OBLIGATORIOS
# ==========================================

@pytest.mark.parametrize(
    "campo",
    ["anho", "id_Mes", "empresaGenera", "planta"]
)
def test_campo_obligatorio_faltante(registro_valido, campo):

    registro_valido.pop(campo)

    errores = validar_registro(
        registro_valido, 2026, 1
    )

    assert any(
        f"falta {campo}" in error
        for error in errores
    )


# ==========================================
# PRUEBA 3: MESES INVALIDOS
# ==========================================

@pytest.mark.parametrize("mes", [0, 13, "abc"])
def test_mes_invalido(registro_valido, mes):

    registro_valido["id_Mes"] = mes

    errores = validar_registro(
        registro_valido, 2026, 1
    )

    assert any(
        "fecha invalida" in error
        for error in errores
    )


# ==========================================
# PRUEBA 4: ANO INCORRECTO
# ==========================================

def test_anho_incorrecto(registro_valido):

    registro_valido["anho"] = 2025

    errores = validar_registro(
        registro_valido, 2026, 1
    )

    assert any(
        "fecha invalida" in error
        for error in errores
    )


# ==========================================
# PRUEBA 5: ENERGIA NO VALIDA
# ==========================================

@pytest.mark.parametrize(
    "valor",
    ["abc", "NaN", "Infinity"]
)
def test_energia_invalida(registro_valido, valor):

    registro_valido["energiaPunta"] = valor

    errores = validar_registro(
        registro_valido, 2026, 1
    )

    assert any(
        "energiaPunta no es numerico" in error
        for error in errores
    )


# ==========================================
# PRUEBA 6: ESTRUCTURA INVALIDA
# ==========================================

def test_estructura_invalida():

    errores = validar_registro(
        None, 2026, 1
    )

    assert any(
        "estructura invalida" in error
        for error in errores
    )
