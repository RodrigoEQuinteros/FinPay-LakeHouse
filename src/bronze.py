# =============================================================
# src/bronze.py 
# Capa Bronze: Ingesta raw de archivos fuente (DLT)
# Cumple el Reto 1: Implementación metadata-driven
# =============================================================

import dlt
from pyspark.sql import functions as F
import json

# -------------------------------------------------------------
# 1. FUNCIONES AUXILIARES (Evita depender de un utils.py externo)
# -------------------------------------------------------------
def load_archetypes(spark, path):
    """Lee el archivo JSON de configuración desde el volumen"""
    try:
        df = spark.read.option("multiline", "true").json(path)
        # Filtra solo las fuentes activas y las convierte a diccionario de Python
        active_sources = df.filter(F.col("active") == True).collect()
        return [row.asDict() for row in active_sources]
    except Exception as e:
        print(f"Error cargando arquetipos desde {path}: {str(e)}")
        raise e

def add_audit_columns(df, source_name):
    """Agrega las columnas técnicas exigidas en la arquitectura Medallion"""
    return (
        df.withColumn("_ingestion_date", F.current_timestamp())
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("_source_system", F.lit(source_name))
    )

# -------------------------------------------------------------
# 2. CARGA DE CONFIGURACIÓN (Reto 1: Metadata-driven)
# -------------------------------------------------------------
ARCHETYPES_PATH = "/Volumes/fintech_finpay/default/vol_landing/metadata/ingestion_archetypes.json"

# Carga la configuración al inicio y crea un diccionario indexado por nombre
_archetypes = {
    a["source_name"]: a
    for a in load_archetypes(spark, ARCHETYPES_PATH)
}

# -------------------------------------------------------------
# 3. MOTOR DE INGESTA DINÁMICA
# -------------------------------------------------------------
def ingest_source(spark, archetype: dict):
    """
    Lee datos usando Auto Loader de Databricks basado en el JSON.
    No hay variables hardcodeadas aquí.
    """
    fmt = archetype.get("file_format", "csv").lower()
    path = archetype["source_path"]
    source_name = archetype["source_name"]
    
    # Auto Loader necesita un lugar para guardar los esquemas inferidos
    schema_loc = f"/Volumes/fintech_finpay/default/vol_landing/metadata/schema/{source_name}/"

    # Preparar el lector en Streaming
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("inferSchema", "false") # En Bronze, todo entra como texto bruto
    )

    if fmt == "csv" or fmt == "text":
        cloud_fmt = "csv"
        delimiter = archetype.get("delimiter", "," if fmt == "csv" else "|")
        header = str(archetype.get("header", True)).lower()
        
        df = (
            reader
            .option("cloudFiles.format", cloud_fmt)
            .option("header", header)
            .option("delimiter", delimiter)
            .load(path)
        )
        
    elif fmt == "json":
        multiline = str(archetype.get("multiline", False)).lower()
        df = (
            reader
            .option("cloudFiles.format", "json")
            .option("multiLine", multiline)
            .load(path)
        )
    else:
        raise ValueError(f"Formato no soportado: {fmt}")

    return add_audit_columns(df, source_name)

# -------------------------------------------------------------
# 4. DEFINICIÓN DE TABLAS BRONZE (Delta Live Tables)
# -------------------------------------------------------------

@dlt.table(
    name="bronze_transactions",
    comment="Transacciones raw ingeridas desde CSV",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    return ingest_source(spark, _archetypes["transactions"])


@dlt.table(
    name="bronze_merchants",
    comment="Comercios raw ingeridos desde JSON",
    table_properties={"quality": "bronze"}
)
def bronze_merchants():
    return ingest_source(spark, _archetypes["merchants"])


@dlt.table(
    name="bronze_users",
    comment="Usuarios raw ingeridos desde TXT con delimitador pipe",
    table_properties={"quality": "bronze"}
)
def bronze_users():
    return ingest_source(spark, _archetypes["users"])