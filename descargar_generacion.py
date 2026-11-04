
import json
from pathlib import Path

import pandas as pd
import requests

BASE_URL = (
    "https://datos.aresep.go.cr/ws.datosabiertos/"
    "Services/IE/Electricidad.svc/"
    "ObtenerGeneracionFuentePlantaYCapacidadInstalada"
)

carpeta = Path("datos")
carpeta.mkdir(exist_ok=True)

def buscar_registros(objeto):
    if isinstance(objeto, list):
        if objeto and isinstance(objeto[0], dict):
            columnas = set(objeto[0])

            if {"anho", "planta"}.issubset(columnas):
                return objeto

        for elemento in objeto:
            resultado = buscar_registros(elemento)
            if resultado is not None:
                return resultado

    elif isinstance(objeto, dict):
        for valor in objeto.values():
            resultado = buscar_registros(valor)
            if resultado is not None:
                return resultado

    return None


todos = []

for anho in range(2018, 2027):
    print(f"Consultando {anho}...")

    url = f"{BASE_URL}/{anho}"

    respuesta = requests.get(url, timeout=120)
    respuesta.raise_for_status()

    contenido = respuesta.json()
    registros = buscar_registros(contenido)

    if not registros:
        raise ValueError(
            f"No se encontraron registros para {anho}. "
            "Debemos revisar cómo funciona el filtro de la API."
        )

    df_anho = pd.DataFrame(registros)
    df_anho["anho"] = pd.to_numeric(
        df_anho["anho"], errors="coerce"
    )

    # Verificar que la API devolvió el año solicitado
    df_anho = df_anho[df_anho["anho"] == anho].copy()

    if df_anho.empty:
        raise ValueError(
            f"La API no devolvió datos de {anho}. "
            "El parámetro de la URL podría no ser el año."
        )

    with open(
        carpeta / f"generacion_{anho}.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(contenido, f, ensure_ascii=False)

    todos.append(df_anho)

    print(f"  Registros encontrados: {len(df_anho)}")

# Unir todos los años
df_final = pd.concat(todos, ignore_index=True)

ruta = carpeta / "generacion_2018_2026.csv"
df_final.to_csv(ruta, index=False, encoding="utf-8-sig")

print("\nDESCARGA FINALIZADA")
print("Total de registros:", len(df_final))
print("\nRegistros por año:")
print(df_final.groupby("anho").size())
print("\nArchivo:", ruta)
