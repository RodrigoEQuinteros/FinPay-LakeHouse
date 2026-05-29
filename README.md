# 🚀 FinPay Lakehouse - Plataforma de Detección de Fraude Transaccional

## 📖 Descripción del Caso de Uso
FinPay es una fintech latinoamericana que procesa pagos digitales. El área de riesgo enfrentaba problemas de visibilidad sobre patrones de fraude debido a procesos manuales y falta de estandarización. 

Este proyecto implementa una plataforma de datos sobre **Azure Databricks** que resuelve este problema mediante un pipeline automatizado de ingesta, procesamiento y publicación analítica. La solución permite al área de riesgo detectar patrones de fraude (alta tasa de reversas, anomalías por comercio) en tiempo casi real utilizando flujos de datos gestionados y observabilidad integrada.

---

## 🏗️ Arquitectura de Datos (Medallion)
El proyecto implementa la arquitectura Medallion utilizando **Databricks Asset Bundles (DAB)** y **Lakeflow Declarative Pipelines (DLT)**:

* **🥉 Capa Bronze:** Ingesta raw sin transformación desde archivos heterogéneos (CSV, JSON, TXT). Implementa un patrón **Metadata-driven** (Reto 1) leyendo la configuración dinámicamente desde un archivo JSON, utilizando Databricks Auto Loader para streaming.
* **🥈 Capa Silver:** Limpieza, estandarización y deduplicación. Implementa reglas de calidad (`@dlt.expect`). Los registros fallidos no se pierden, sino que se envían a una **Tabla de Cuarentena** (Reto 2) para auditoría. Los datos PII de usuarios están protegidos mediante *Column Masking* y *Row-Level Security* en Unity Catalog.
* **🥇 Capa Gold:** Modelo dimensional (Star Schema) y tablas de agregación con KPIs de riesgo. Se utilizan *Materialized Views* ejecutadas sobre un SQL Warehouse para garantizar el máximo rendimiento analítico.

---

## 📂 Estructura del Repositorio

```text
finpay-lakehouse/
├── resources/                    # Recursos de Databricks (DABs)
│   ├── finpay_etl_pipeline.yml   # Pipeline DLT (Bronze -> Silver -> Gold)
│   ├── finpay_ingestion_job.yml  # Job 1: Orquesta el pipeline ETL
│   ├── finpay_semantic_job.yml   # Job 2: Construye el modelo semántico
│   └── finpay_observability_dashboard.yml # Despliegue del Dashboard
├── src/                          # Código fuente (PySpark / DLT)
│   ├── bronze.py                 # Lógica de ingesta metadata-driven
│   ├── silver.py                 # Transformación y tabla de cuarentena
│   ├── gold.py                   # Agregaciones y KPIs de fraude
│   └── utils.py                  # Funciones y constantes compartidas
├── notebooks/                    # Notebooks de configuración y SQL
│   ├── 00_setup.ipynb            # Aprovisionamiento (Catálogo, Volumen, Permisos)
│   ├── 01_create_materialized_views.ipynb # Creación del modelo dimensional
│   ├── 02_refresh_materialized_views.ipynb # Actualización de vistas materializadas
│   └── 03_observability_queries.ipynb # Consultas de diagnóstico del pipeline
├── dashboard/                    
│   └── observability.lvdash.json # Export del dashboard AI/BI de observabilidad
├── databricks.yml                # Configuración principal del Bundle
└── README.md                     # Documentación del proyecto