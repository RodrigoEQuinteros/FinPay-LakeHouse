# =============================================================
# src/silver.py
# Capa Silver: Limpieza, deduplicación y enriquecimiento
# Cumple el Reto 2: Tabla de Cuarentena para registros fallidos
# =============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# -------------------------------------------------------------
# 1. FUNCIONES AUXILIARES DE LIMPIEZA
# -------------------------------------------------------------
def clean_amount(col_name):
    """Convierte el monto de STRING a DECIMAL, limpiando caracteres extraños"""
    cleaned = F.regexp_replace(F.col(col_name), r"[^\d\.]", "")
    return cleaned.cast(DecimalType(18, 2))

def parse_mixed_dates(col_name):
    """Maneja las fechas sucias que vienen en formatos mixtos"""
    return F.coalesce(
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "yyyy-MM-dd")
    )

def build_quarantine_record(df, source_name, reject_reason):
    """Prepara los registros rechazados con el formato exacto del Reto 2"""
    return (
        df.withColumn("fuente_origen", F.lit(source_name))
          .withColumn("motivo_rechazo", F.lit(reject_reason))
          .withColumn("fecha_procesamiento", F.current_timestamp())
          .withColumn("registro_original", F.to_json(F.struct("*")))
          .select("fuente_origen", "motivo_rechazo", "fecha_procesamiento", "registro_original")
    )

# -------------------------------------------------------------
# 2. CAPA SILVER: TRANSACTIONS
# -------------------------------------------------------------
@dlt.table(
    name="silver_transactions",
    comment="Transacciones curadas, deduplicadas y tipadas",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("transaction_id_valido", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("user_id_valido", "user_id IS NOT NULL")
@dlt.expect_or_drop("merchant_id_valido", "merchant_id IS NOT NULL")
@dlt.expect_or_drop("monto_positivo", "amount > 0")
@dlt.expect_or_drop("fecha_valida", "transaction_date IS NOT NULL")
def silver_transactions():
    df = dlt.read_stream("bronze_transactions")
    
    return (
        df
        # Limpieza de tipos
        .withColumn("amount", clean_amount("amount"))
        .withColumn("transaction_date", parse_mixed_dates("transaction_date"))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("transaction_type", F.lower(F.trim(F.col("transaction_type"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        
        # Regla de negocio: reversas deben tener reference_id
        .withColumn("reference_id", 
            F.when(
                (F.col("transaction_type") == "reversa") & F.col("reference_id").isNull(), 
                F.lit("SIN_REFERENCIA")
            ).otherwise(F.col("reference_id"))
        )
        
        # Deduplicación guardando el registro más reciente
        .withWatermark("_ingestion_date", "1 hour")
        .dropDuplicates(["transaction_id"])
        
        .select(
            "transaction_id", "user_id", "merchant_id", "channel", 
            "transaction_type", "amount", "currency", "transaction_date", 
            "status", "reference_id", "_ingestion_date"
        )
    )

# -------------------------------------------------------------
# 3. CAPA SILVER: MERCHANTS
# -------------------------------------------------------------
@dlt.table(
    name="silver_merchants",
    comment="Comercios limpios y categorizados",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("merchant_id_valido", "merchant_id IS NOT NULL")
@dlt.expect_or_drop("nombre_valido", "merchant_name IS NOT NULL")
def silver_merchants():
    df = dlt.read_stream("bronze_merchants")
    
    return (
        df
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("risk_level", F.lower(F.trim(F.col("risk_level"))))
        .withColumn("affiliation_date", parse_mixed_dates("affiliation_date"))
        
        # Deduplicación
        .withWatermark("_ingestion_date", "1 hour")
        .dropDuplicates(["merchant_id"])
        
        .select(
            "merchant_id", "merchant_name", "category", "country", 
            "affiliation_date", "status", "risk_level", "_ingestion_date"
        )
    )

# -------------------------------------------------------------
# 4. CAPA SILVER: USERS
# -------------------------------------------------------------
@dlt.table(
    name="silver_users",
    comment="Usuarios limpios. (El Column Masking PII se aplica en UC)",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("user_id_valido", "user_id IS NOT NULL")
@dlt.expect_or_drop("email_valido", "email IS NOT NULL")
def silver_users():
    df = dlt.read_stream("bronze_users")
    
    return (
        df
        .withColumn("full_name", F.trim(F.col("full_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("registration_date", parse_mixed_dates("registration_date"))
        
        # Deduplicación
        .withWatermark("_ingestion_date", "1 hour")
        .dropDuplicates(["user_id"])
        
        .select(
            "user_id", "full_name", "document_id", "email", 
            "phone", "country", "segment", "registration_date", "_ingestion_date"
        )
    )

# -------------------------------------------------------------
# 5. RETO 2: TABLA DE CUARENTENA
# -------------------------------------------------------------
@dlt.table(
    name="quarantine",
    comment="Registros que no superaron las reglas críticas de calidad",
    table_properties={"quality": "silver"}
)
def silver_quarantine():
    # Detectar transacciones fallidas
    tx = dlt.read_stream("bronze_transactions")
    bad_tx = tx.filter(
        F.col("transaction_id").isNull() | 
        F.col("user_id").isNull() | 
        F.col("merchant_id").isNull() | 
        (clean_amount("amount") <= 0)
    )
    q_tx = build_quarantine_record(bad_tx, "transactions", "Campos clave nulos o monto inválido")

    # Detectar usuarios fallidos
    users = dlt.read_stream("bronze_users")
    bad_users = users.filter(F.col("user_id").isNull() | F.col("email").isNull())
    q_users = build_quarantine_record(bad_users, "users", "Usuario sin ID o sin Email")

    # Detectar comercios fallidos
    merchants = dlt.read_stream("bronze_merchants")
    bad_merchants = merchants.filter(F.col("merchant_id").isNull() | F.col("merchant_name").isNull())
    q_merchants = build_quarantine_record(bad_merchants, "merchants", "Comercio sin ID o sin Nombre")

    # Unificar todos los rechazos en una sola tabla maestra de auditoría
    return q_tx.union(q_users).union(q_merchants)