
import subprocess
import sys
from pathlib import Path

# Directorio principal del proyecto
BASE = Path(__file__).resolve().parent


def ejecutar(nombre, comando, **kwargs):
    print(f"\n--- {nombre} ---", flush=True)

    subprocess.run(
        comando,
        cwd=BASE,
        check=True,
        **kwargs
    )


# ETAPA 1: EXTRAER
ejecutar(
    "EXTRACCION DE ARESEP",
    [sys.executable, "descargar_generacion.py"]
)

# ETAPA 2: CARGAR
ejecutar(
    "CARGA EN POSTGRESQL",
    [sys.executable, "cargar_raw.py"]
)

# ETAPA 3: TRANSFORMAR
archivo_sql = BASE / "sql" / "transformaciones.sql"

ejecutar(
    "TRANSFORMACIONES SQL",
    [
        "docker", "exec", "-i",
        "ice-postgres",
        "psql",
        "-X",
        "-v", "ON_ERROR_STOP=1",
        "-U", "ice_user",
        "-d", "ice_data"
    ],
    input=archivo_sql.read_bytes()
)

print("\nPIPELINE FINALIZADO CORRECTAMENTE")
