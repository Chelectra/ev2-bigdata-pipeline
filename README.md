# ev2-bigdata-pipeline
Pipeline de Big Data AWS (S3 + PySpark + Athena) - EV2 :)
# Pipeline de Big Data de Alta Disponibilidad en AWS (EV2 - TI3025)

Repositorio oficial del proyecto correspondiente a la **Evaluación Sumativa 02 (25%)** de la asignatura **Big Data (TI3025)** de la carrera de **Ingeniería en Informática** en **INACAP Sede Punta Arenas**.

* **Integrantes:** Ricardo Cayupel & Lorenzo Aguila
* **Docente:** Paulo Simunovic Oyarzún
* **Fecha de Entrega:** 19 de Octubre de 2026
* **Opción Técnica Implementada:** Opción A — AWS Free Tier (`Amazon S3` + `PySpark en Google Colab` + `Amazon Athena`)

---

## Arquitectura del Pipeline (4 Capas)
1. **Capa de Ingesta (Batch):** Extracción programática del dataset abierto *NYC TLC Yellow Taxi Trip Records* mediante Python y `boto3` hacia la Zona Bronce (`raw/`) en Amazon S3.
2. **Capa de Almacenamiento (Data Lake Multi-AZ):** Bucket `ev2-bigdata-cayupel-aguila-2026` en **Amazon S3 Standard** con replicación síncrona en múltiples Zonas de Disponibilidad (99.999999999% de durabilidad).
3. **Capa de Procesamiento (ETL con PySpark):** Limpieza de valores atípicos, ingeniería de características, conversión al formato columnar **Apache Parquet con compresión Snappy** y particionamiento físico por `payment_type_str` en la Zona Plata (`curated/`).
4. **Capa de Análisis y Modelamiento (ML / BI):**
   * **Análisis Descriptivo (EDA Distribuido):** Consultas aglomeradas ejecutadas en **PySpark SQL** y **Amazon Athena**.
   * **Modelo Predictivo (`pyspark.ml`):** Pipeline distribuido integrado por `Imputer`, `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` y `RandomForestRegressor`, evaluado formalmente con **RMSE, MAE y R²**.

---

## Estructura del Repositorio
```text
ev2-bigdata-pipeline-aws/
├── config/
│   └── config.yaml                  # Parámetros de AWS S3, PySpark y Machine Learning
├── notebooks/
│   └── EV2_BigData_Pipeline.ipynb   # Notebook interactivo ejecutado en Google Colab
├── sql/
│   └── athena_queries.sql           # Consultas DDL y analíticas para Amazon Athena
├── src/
│   └── pipeline_bigdata_ev2.py      # Script modular completo en Python / PySpark
├── .gitignore                       # Exclusión de credenciales IAM y datos pesados
├── requirements.txt                 # Dependencias y versiones del entorno
└── README.md                        # Documentación general del proyecto
