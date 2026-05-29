# =============================================================
# src/gold.py 
# Capa Gold: Agregaciones analíticas y reglas de negocio
# KPIs de riesgo, tasas de reversa y detección de anomalías
# =============================================================

import dlt
from pyspark.sql import functions as F

# -------------------------------------------------------------
# 1. TABLA GOLD: KPIs DE RIESGO
# -------------------------------------------------------------
@dlt.table(
    name="risk_kpis",
    comment="KPIs de riesgo por comercio y canal — tasa de reversa y score",
    table_properties={"quality": "gold"}
)
def gold_risk_kpis():
    tx = dlt.read_stream("silver_transactions")
    merchants = dlt.read("silver_merchants")

    # ── Agregar métricas por comercio y canal ──
    kpis = (
        tx
        .groupBy(
            "merchant_id",
            "channel",
            "currency",
            F.col("transaction_date").alias("fecha")
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum(
                F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)
            ).alias("total_reversas"),
            F.sum(
                F.when(F.col("status") == "aprobado", F.col("amount")).otherwise(0)
            ).alias("monto_aprobado"),
        )
    )

    # ── Calcular tasa de reversa y score de riesgo ──
    kpis = (
        kpis
        .withColumn(
            "tasa_reversa",
            F.round(F.col("total_reversas") / F.col("total_transactions"), 4)
        )
        # Score de riesgo: pondera tasa de reversa (70%) y monto promedio (30%)
        .withColumn(
            "score_riesgo",
            F.round(
                (F.col("tasa_reversa") * 70) +
                (F.when(F.col("avg_amount") > 5000, 30)
                 .when(F.col("avg_amount") > 1000, 15)
                 .otherwise(5)),
                2
            )
        )
    )

    # ── Enriquecer con datos del comercio ──
    return (
        kpis
        .join(
            merchants.select("merchant_id", "merchant_name", "category", "country", "risk_level"),
            on="merchant_id",
            how="left"
        )
        .select(
            "merchant_id", "merchant_name", "category", "country", "channel", 
            "currency", "fecha", "total_transactions", "total_amount", 
            "monto_aprobado", "avg_amount", "total_reversas", "tasa_reversa", 
            "score_riesgo", "risk_level"
        )
    )

# -------------------------------------------------------------
# 2. TABLA GOLD: ANOMALÍAS (Posible Fraude)
# -------------------------------------------------------------
@dlt.table(
    name="anomalies",
    comment="Comercios con patrones anómalos de reversa",
    table_properties={"quality": "gold"}
)
def gold_anomalies():
    kpis = dlt.read("risk_kpis")

    return (
        kpis
        # ── Clasificar severidad de la anomalía ──
        .withColumn(
            "severidad",
            F.when(F.col("tasa_reversa") >= 0.30, "CRITICA")
            .when(F.col("tasa_reversa") >= 0.15, "ALTA")
            .when(F.col("tasa_reversa") >= 0.05, "MEDIA")
            .otherwise("NORMAL")
        )
        # ── Filtrar solo anomalías reales ──
        .filter(F.col("severidad") != "NORMAL")
        .withColumn("detected_at", F.current_timestamp())
        .select(
            "merchant_id", "merchant_name", "category", "country", "channel",
            "fecha", "total_transactions", "total_reversas", "tasa_reversa",
            "score_riesgo", "severidad", "detected_at"
        )
    )

# -------------------------------------------------------------
# 3. TABLA GOLD: RESUMEN POR CANAL
# -------------------------------------------------------------
@dlt.table(
    name="channel_summary",
    comment="Métricas agregadas por canal (web, app, pos)",
    table_properties={"quality": "gold"}
)
def gold_channel_summary():
    tx = dlt.read_stream("silver_transactions")

    return (
        tx
        .withWatermark("_ingestion_date", "1 hour")
        .groupBy("channel", F.col("transaction_date").alias("fecha"))
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("total_reversas"),
            F.sum(F.when(F.col("status") == "aprobado", 1).otherwise(0)).alias("total_aprobadas"),
            F.approx_count_distinct("merchant_id").alias("merchants_activos"),
            F.approx_count_distinct("user_id").alias("usuarios_activos"),
        )
        .withColumn("tasa_reversa", F.round(F.col("total_reversas") / F.col("total_transactions"), 4))
        .withColumn("tasa_aprobacion", F.round(F.col("total_aprobadas") / F.col("total_transactions"), 4))
    )