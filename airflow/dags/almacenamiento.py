"""JSON originales y snapshots Parquet del STAGING de dbt en S3/MinIO.

No importa Airflow. Boto3 y PyArrow se importan al ejecutar las funciones,
para evitar trabajo innecesario durante el descubrimiento de los DAGs.
"""

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal


MODELO_STAGING = "dbt_dev_staging.stg_generacion_planta"
PREFIJO_RAW = "raw/generacion_planta"
PREFIJO_PROCESSED = "processed/generacion_planta"
VERSION_ESQUEMA = "1"
CAMPOS_DECIMALES = (
    "energia_punta", "energia_valle", "energia_noche", "factor_carga"
)


def id_ejecucion(dag_id, run_id):
    """Identificador estable que no introduce barras ni caracteres especiales."""
    contenido = json.dumps([dag_id, run_id], ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(contenido).hexdigest()


def esquema_parquet():
    """Contrato v1: decimales exactos, hasta 26 enteros y 12 decimales."""
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("raw_id", pa.int64(), nullable=False),
            pa.field("anho", pa.int16(), nullable=False),
            pa.field("id_mes", pa.int16(), nullable=False),
            pa.field("empresa_genera", pa.string(), nullable=False),
            pa.field("planta", pa.string(), nullable=False),
            *[
                pa.field(nombre, pa.decimal128(38, 12), nullable=True)
                for nombre in CAMPOS_DECIMALES
            ],
            pa.field("contenido_hash", pa.string(), nullable=False),
        ],
        metadata={b"aresep_schema_version": VERSION_ESQUEMA.encode("ascii")},
    )


def construir_tabla(registros):
    """Valida el contrato de exportacion sin volver a transformar el STAGING."""
    import pyarrow as pa

    if not registros:
        raise ValueError("El STAGING esta vacio; no se publica un snapshot vacio.")

    esquema = esquema_parquet()
    columnas = set(esquema.names)
    hashes = set()
    identificadores = set()

    for numero, registro in enumerate(registros, start=1):
        if set(registro) != columnas:
            faltantes = sorted(columnas - set(registro))
            adicionales = sorted(set(registro) - columnas)
            raise ValueError(
                f"Esquema STAGING incompatible en fila {numero}: "
                f"faltantes={faltantes}, adicionales={adicionales}"
            )

        for campo in esquema:
            if not campo.nullable and registro[campo.name] is None:
                raise ValueError(f"Fila {numero}: {campo.name} no admite nulos.")

        for campo in ("raw_id", "anho", "id_mes"):
            if type(registro[campo]) is not int:
                raise ValueError(f"Fila {numero}: {campo} debe ser entero.")

        if not 2018 <= registro["anho"] <= 9999:
            raise ValueError(f"Fila {numero}: anho fuera de rango.")
        if not 1 <= registro["id_mes"] <= 12:
            raise ValueError(f"Fila {numero}: id_mes fuera de rango.")
        if registro["raw_id"] < 1:
            raise ValueError(f"Fila {numero}: raw_id debe ser positivo.")

        for campo in ("empresa_genera", "planta"):
            valor = registro[campo]
            if not isinstance(valor, str) or not valor.strip():
                raise ValueError(f"Fila {numero}: {campo} debe contener texto.")

        huella = registro["contenido_hash"]
        if not isinstance(huella, str) or not re.fullmatch(r"[0-9a-f]{64}", huella):
            raise ValueError(f"Fila {numero}: contenido_hash invalido.")
        if huella in hashes or registro["raw_id"] in identificadores:
            raise ValueError(f"Fila {numero}: hash o raw_id duplicado.")
        hashes.add(huella)
        identificadores.add(registro["raw_id"])

        for campo in CAMPOS_DECIMALES:
            valor = registro[campo]
            if valor is not None and (
                not isinstance(valor, Decimal) or not valor.is_finite()
            ):
                raise ValueError(f"Fila {numero}: {campo} debe ser Decimal finito o nulo.")

    # Arrow rechaza escalas/precisiones que no caben en decimal(38,12),
    # en lugar de redondear silenciosamente.
    tabla = pa.Table.from_pylist(registros, schema=esquema)
    tabla.validate(full=True)
    return tabla.sort_by([("anho", "ascending"), ("contenido_hash", "ascending")])


class AlmacenamientoS3:
    """Cliente del bucket. Permite inyectar un cliente en las pruebas."""

    def __init__(self, cliente=None, bucket=None):
        self.bucket = bucket or os.environ["S3_BUCKET"]

        if cliente is None:
            import boto3
            from botocore.config import Config

            cliente = boto3.client(
                "s3",
                endpoint_url=os.environ["S3_ENDPOINT_URL"],
                aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                region_name=os.environ.get("S3_REGION", "us-east-1"),
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    connect_timeout=10,
                    read_timeout=60,
                    retries={"mode": "standard", "total_max_attempts": 3},
                ),
            )
        self.cliente = cliente

    def guardar_bytes(self, clave, contenido, tipo):
        huella = hashlib.sha256(contenido).hexdigest()
        self.cliente.put_object(
            Bucket=self.bucket,
            Key=clave,
            Body=contenido,
            ContentType=tipo,
            Metadata={"sha256": huella},
        )
        return {
            "bucket": self.bucket,
            "key": clave,
            "sha256": huella,
            "bytes": len(contenido),
        }

    def guardar_json(self, clave, documento):
        contenido = json.dumps(
            documento, ensure_ascii=False, sort_keys=True, allow_nan=False
        ).encode("utf-8")
        return self.guardar_bytes(clave, contenido, "application/json")

    def leer_bytes(self, referencia):
        if referencia["bucket"] != self.bucket:
            raise ValueError("El bucket de la referencia no coincide con S3_BUCKET.")

        respuesta = self.cliente.get_object(
            Bucket=self.bucket, Key=referencia["key"]
        )
        with respuesta["Body"] as cuerpo:
            contenido = cuerpo.read()

        if (
            len(contenido) != referencia["bytes"]
            or hashlib.sha256(contenido).hexdigest() != referencia["sha256"]
        ):
            raise ValueError(f"Integridad SHA-256 incorrecta: {referencia['key']}")
        return contenido

    def leer_json(self, referencia):
        return json.loads(self.leer_bytes(referencia))

    def guardar_raw(self, dag_id, run_id, anho, contenido):
        ejecucion = id_ejecucion(dag_id, run_id)
        huella = hashlib.sha256(contenido).hexdigest()
        clave = (
            f"{PREFIJO_RAW}/ejecucion={ejecucion}/"
            f"anho={anho}/respuesta_{huella}.json"
        )
        return self.guardar_bytes(clave, contenido, "application/json")

    def publicar_extraccion(self, dag_id, run_id, modo, lotes):
        ejecucion = id_ejecucion(dag_id, run_id)
        documento = {
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "modo": modo,
            "creado_en_utc": datetime.now(timezone.utc).isoformat(),
            "registros_recibidos": sum(lote["registros"] for lote in lotes),
            "lotes": lotes,
        }
        return self.guardar_json(
            f"{PREFIJO_RAW}/ejecucion={ejecucion}/manifest.json", documento
        )

    def publicar_snapshot(self, registros, dag_id, run_id, extraccion=None):
        """Publica el manifiesto solo despues de verificar TODOS los Parquet."""
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        tabla = construir_tabla(registros)
        ejecucion = id_ejecucion(dag_id, run_id)
        prefijo = f"{PREFIJO_PROCESSED}/ejecucion={ejecucion}"
        archivos = []

        for anho in sorted(tabla.column("anho").unique().to_pylist()):
            particion = tabla.filter(pc.equal(tabla.column("anho"), anho))
            salida = pa.BufferOutputStream()
            pq.write_table(particion, salida, compression="snappy")
            contenido = salida.getvalue().to_pybytes()
            huella = hashlib.sha256(contenido).hexdigest()

            referencia = self.guardar_bytes(
                f"{prefijo}/anho={anho}/part_{huella}.parquet",
                contenido,
                "application/octet-stream",
            )

            # La validacion lee el objeto REAL del storage, no solo el buffer local.
            recuperada = pq.read_table(pa.BufferReader(self.leer_bytes(referencia)))
            recuperada.validate(full=True)
            if not particion.equals(recuperada, check_metadata=True):
                raise ValueError(f"El Parquet de {anho} no conserva datos y esquema.")

            archivos.append({**referencia, "anho": anho, "filas": particion.num_rows})

        manifiesto = {
            "schema_version": VERSION_ESQUEMA,
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "creado_en_utc": datetime.now(timezone.utc).isoformat(),
            "modelo_dbt": MODELO_STAGING,
            "alcance": "snapshot_completo_staging_postgres",
            "extraccion_de_esta_ejecucion": extraccion,
            "filas_total": tabla.num_rows,
            "columnas": [
                {"nombre": campo.name, "tipo": str(campo.type), "nullable": campo.nullable}
                for campo in tabla.schema
            ],
            "validacion": "contrato_v1_sha256_y_lectura_parquet_desde_s3",
            "archivos": archivos,
        }

        referencia_manifest = self.guardar_json(f"{prefijo}/manifest.json", manifiesto)
        # Un solo PUT publica la lista completa: el consumidor abre exclusivamente
        # los archivos de este manifiesto, sin mezclar distintas ejecuciones.
        self.guardar_json(f"{PREFIJO_PROCESSED}/latest.json", manifiesto)
        return {"manifest": referencia_manifest, "filas": tabla.num_rows, "archivos": archivos}
