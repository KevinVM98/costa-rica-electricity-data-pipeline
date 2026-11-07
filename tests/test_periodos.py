
import sys
from pathlib import Path

import pytest


# Agregamos la carpeta del DAG a la ruta de Python.
RUTA_DAGS = (
    Path(__file__).resolve().parents[1]
    / "airflow"
    / "dags"
)

sys.path.insert(0, str(RUTA_DAGS))

# Importamos nuestra función real, independiente de Airflow.
from periodos import seleccionar_anhos


# PRUEBA 1: Modo mensual
def test_modo_mensual():
    resultado = seleccionar_anhos("mensual", 2026)

    assert resultado == [2025, 2026]


# PRUEBA 2: Modo histórico
def test_modo_historico():
    resultado = seleccionar_anhos("historico", 2026)

    assert resultado == list(range(2018, 2027))


# PRUEBA 3: Modo inválido
def test_modo_invalido():
    with pytest.raises(ValueError):
        seleccionar_anhos("invalido", 2026)
