# =============================================================
# src/utils.py
# Funciones y constantes reutilizables compartidas por el pipeline
# =============================================================

from pyspark.sql import functions as F

# -------------------------------------------------------------
# CONSTANTES GLOBALES DEL MODELO DE DATOS
# -------------------------------------------------------------
CATALOG_NAME = "fintech_finpay"
VALID_CURRENCIES = ["PEN", "USD", "COP", "MXN", "CLP", "ARS"]
VALID_CHANNELS = ["web", "app", "pos"]
VALID_TX_TYPES = ["pago", "reversa", "retiro"]
VALID_STATUS = ["aprobado", "rechazado", "pendiente"]
VALID_COUNTRIES = ["PE", "CO", "MX", "CL", "AR"]

# -------------------------------------------------------------
# FUNCIONES DE UTILIDAD GENERAL
# -------------------------------------------------------------
def get_audit_columns(df, source_name: str):
    """
    Función base para agregar columnas de auditoría.
    Aplicable en cualquier capa del Lakehouse.
    """
    return (
        df
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_processed_timestamp", F.current_timestamp())
    )

def standardize_iso_country(col_name: str):
    """
    Convierte nombres de países a códigos ISO de 2 letras.
    """
    return (
        F.when(F.lower(F.trim(F.col(col_name))) == "peru", F.lit("PE"))
        .when(F.lower(F.trim(F.col(col_name))) == "perú", F.lit("PE"))
        .when(F.lower(F.trim(F.col(col_name))) == "colombia", F.lit("CO"))
        .when(F.lower(F.trim(F.col(col_name))) == "mexico", F.lit("MX"))
        .when(F.lower(F.trim(F.col(col_name))) == "méxico", F.lit("MX"))
        .when(F.lower(F.trim(F.col(col_name))) == "chile", F.lit("CL"))
        .when(F.lower(F.trim(F.col(col_name))) == "argentina", F.lit("AR"))
        .otherwise(F.upper(F.trim(F.col(col_name))))
    )