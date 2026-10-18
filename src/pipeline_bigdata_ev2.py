#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
PROYECTO: Pipeline de Big Data de Alta Disponibilidad en AWS Free Tier
INTEGRANTES: Ricardo Cayupel y Lorenzo Aguila
===============================================================================
Descripción:
Script principal que implementa las 4 capas de la arquitectura de Big Data:
1. Ingesta Batch desde repositorio oficial NYC TLC hacia Amazon S3 (Zona Raw).
2. Almacenamiento distribuido en Data Lake S3 (Bronce y Plata).
3. Procesamiento ETL en PySpark y conversión a formato Parquet + Snappy
   con particionamiento físico estilo Hive (payment_type_str).
4. Análisis Descriptivo (PySpark SQL) y Modelo Predictivo (pyspark.ml)
   con Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
   y RandomForestRegressor validado con RMSE, MAE y R2.
===============================================================================
"""

import os
import urllib.request
from getpass import getpass
import boto3
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def init_aws_and_spark():
    """
    Inicializa el cliente de Amazon S3 (usando credenciales de 'bigdata-user' IAM)
    y la sesión distribuida de Apache Spark con compresión Snappy (Criterio 2.1.2).
    """
    print("[PASO 1] Configurando credenciales IAM ('bigdata-user') y sesión PySpark...")
    aws_access_key = getpass("Ingresa tu AWS Access Key ID (ej. AKIAX5UZYCO6IPSAWOOG): ")
    aws_secret_key = getpass("Ingresa tu AWS Secret Access Key: ")
    aws_region = "us-east-1"
    s3_bucket = "ev2-bigdata-cayupel-aguila"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    spark = SparkSession.builder \
        .appName("EV2_BigData_AWS_Pipeline_Cayupel_Aguila") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    print(f"Sesión de PySpark iniciada con éxito. Versión: {spark.version}")
    return s3_client, s3_bucket, spark


def ingest_raw_data(s3_client, s3_bucket, spark):
    """
    Descarga el dataset de viajes en taxi de NYC y lo respalda en la Zona Raw
    (Bronce) del Data Lake en Amazon S3 (Criterio 2.1.1 y 2.1.2).
    """
    print("\n[PASO 2] Ejecutando Capa de Ingesta Batch hacia Amazon S3...")
    os.makedirs("datalake/raw", exist_ok=True)
    os.makedirs("datalake/curated", exist_ok=True)

    url_dataset = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    local_raw_path = "datalake/raw/yellow_tripdata_2024-01.parquet"

    if not os.path.exists(local_raw_path):
        print("Descargando dataset oficial NYC TLC (Enero 2024)...")
        urllib.request.urlretrieve(url_dataset, local_raw_path)
        print("Descarga local completada.")

    print(f"☁️ Subiendo archivo crudo a s3://{s3_bucket}/raw/yellow_tripdata_2024-01.parquet ...")
    s3_client.upload_file(local_raw_path, s3_bucket, "raw/yellow_tripdata_2024-01.parquet")
    print("Respaldo en Zona Raw (Bronce) de Amazon S3 finalizado.")

    df_raw = spark.read.parquet(local_raw_path)
    print(f"📦 Total de registros brutos leídos en el cluster: {df_raw.count():,}")

    # Muestra representativa del 5% (~150,000 registros) para ejecución óptima en Free Tier
    df_sample = df_raw.sample(withReplacement=False, fraction=0.05, seed=42)
    return df_sample


def process_and_partition_etl(df_sample, s3_client, s3_bucket):
    """
    Ejecuta limpieza ETL distribuida en PySpark, guarda en formato Parquet con
    compresión Snappy particionado por 'payment_type_str' y sincroniza con S3 (Criterio 2.1.2).
    """
    print("\n[PASO 3] Ejecutando ETL en PySpark y Particionamiento Parquet...")

    df_clean = df_sample.withColumn(
        "trip_duration_min",
        F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2)
    ).withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    ).withColumn(
        "pickup_day_of_week", F.dayofweek("tpep_pickup_datetime")
    ).withColumn(
        "payment_type_str",
        F.when(F.col("payment_type") == 1, "Credit_Card")
         .when(F.col("payment_type") == 2, "Cash")
         .otherwise("Other")
    ).filter(
        (F.col("trip_distance") >= 0.2) & (F.col("trip_distance") <= 80.0) &
        (F.col("fare_amount") >= 3.0) & (F.col("fare_amount") <= 250.0) &
        (F.col("trip_duration_min") >= 1.0) & (F.col("trip_duration_min") <= 180.0)
    ).select(
        F.col("VendorID").cast("int"),
        F.col("passenger_count").cast("double"),
        F.col("trip_distance").cast("double"),
        F.col("RatecodeID").cast("double"),
        F.col("pickup_hour").cast("int"),
        F.col("pickup_day_of_week").cast("int"),
        F.col("trip_duration_min").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("fare_amount").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("payment_type_str")
    )

    curated_local_dir = "datalake/curated/nyc_trips"
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("payment_type_str") \
        .parquet(curated_local_dir)

    # Subida de particiones Parquet limpias (omitiendo archivos temporales .crc de Spark)
    uploaded_count = 0
    for root, _, files in os.walk(curated_local_dir):
        for file in files:
            if not file.endswith(".crc") and not file.startswith("_"):
                full_local_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_local_path, curated_local_dir)
                s3_key = f"curated/nyc_trips/{rel_path}".replace("\\", "/")
                s3_client.upload_file(full_local_path, s3_bucket, s3_key)
                uploaded_count += 1

    print(f"{uploaded_count} archivos Parquet particionados subidos a s3://{s3_bucket}/curated/nyc_trips/")
    return curated_local_dir


def run_descriptive_analytics(spark, curated_local_dir):
    """
    Ejecuta el Análisis Descriptivo Distribuido (EDA) mediante PySpark SQL
    y exporta visualizaciones estadísticas (Criterio 2.1.3).
    """
    print("\n[PASO 4] Ejecutando Análisis Descriptivo (EDA Distribuido en PySpark SQL)...")
    df_curated = spark.read.parquet(curated_local_dir)
    df_curated.createOrReplaceTempView("vw_nyc_trips")

    query_1 = """
    SELECT 
        payment_type_str AS Metodo_Pago,
        COUNT(*) AS Total_Viajes,
        ROUND(AVG(trip_distance), 2) AS Distancia_Media_Mi,
        ROUND(AVG(trip_duration_min), 2) AS Duracion_Media_Min,
        ROUND(AVG(fare_amount), 2) AS Tarifa_Media_USD,
        ROUND(STDDEV(fare_amount), 2) AS Desv_Est_Tarifa_USD,
        ROUND(AVG(tip_amount), 2) AS Propina_Media_USD
    FROM vw_nyc_trips
    GROUP BY payment_type_str
    ORDER BY Total_Viajes DESC
    """
    print("\n--- Tabla 1: Métricas Estadísticas por Método de Pago ---")
    spark.sql(query_1).show(truncate=False)

    query_2 = """
    SELECT 
        pickup_hour AS Hora_Dia,
        COUNT(*) AS Cantidad_Viajes,
        ROUND(AVG(fare_amount), 2) AS Tarifa_Promedio_USD,
        ROUND(AVG(trip_duration_min), 2) AS Duracion_Promedio_Min
    FROM vw_nyc_trips
    GROUP BY pickup_hour
    ORDER BY pickup_hour ASC
    """
    print("\n--- Tabla 2: Comportamiento Horario de Demanda y Duración ---")
    df_hourly = spark.sql(query_2)
    df_hourly.show(24, truncate=False)

    # Generación de gráfico combinado para el informe técnico
    pdf_hourly = df_hourly.toPandas()
    fig, ax1 = plt.subplots(figsize=(11, 5))
    sns.barplot(data=pdf_hourly, x="Hora_Dia", y="Cantidad_Viajes", color="#4C72B0", alpha=0.75, ax=ax1)
    ax1.set_xlabel("Hora del Día (00:00 a 23:00 hrs)")
    ax1.set_ylabel("Cantidad de Viajes", color="#4C72B0", fontweight="bold")

    ax2 = ax1.twinx()
    sns.lineplot(data=pdf_hourly, x="Hora_Dia", y="Duracion_Promedio_Min", color="#C44E52", marker="o", linewidth=2.5, ax=ax2)
    ax2.set_ylabel("Duración Promedio del Viaje (Minutos)", color="#C44E52", fontweight="bold")

    plt.title("EDA Distribuido Big Data: Demanda Horaria vs Duración Media del Viaje")
    plt.tight_layout()
    plt.savefig("eda_demanda_duracion.png", dpi=150)
    print("Gráfico exportado exitosamente como 'eda_demanda_duracion.png'")
    return df_curated


def run_predictive_model(df_curated):
    """
    Construye, entrena y evalúa el modelo predictivo RandomForestRegressor
    utilizando un Pipeline de pyspark.ml (Criterio 2.1.3).
    """
    print("\n[PASO 5] Entrenando Modelo Predictivo Distribuido con pyspark.ml...")
    train_df, test_df = df_curated.randomSplit([0.8, 0.2], seed=42)

    # 1. Imputación de valores nulos con la mediana
    imputer = Imputer(
        inputCols=["passenger_count", "RatecodeID"],
        outputCols=["passenger_count_imp", "RatecodeID_imp"]
    ).setStrategy("median")

    # 2. Codificación categórica (StringIndexer + OneHotEncoder)
    indexer = StringIndexer(
        inputCol="payment_type_str",
        outputCol="payment_type_idx",
        handleInvalid="keep"
    )
    encoder = OneHotEncoder(
        inputCols=["payment_type_idx"],
        outputCols=["payment_type_ohe"]
    )

    # 3. Ensamblador de características (VectorAssembler)
    feature_cols = [
        "trip_distance",
        "trip_duration_min",
        "passenger_count_imp",
        "RatecodeID_imp",
        "pickup_hour",
        "pickup_day_of_week",
        "payment_type_ohe"
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    # 4. Escalado estándar de vectores
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False
    )

    # 5. Algoritmo Random Forest Regressor
    rf_model = RandomForestRegressor(
        featuresCol="features",
        labelCol="fare_amount",
        numTrees=50,
        maxDepth=8,
        seed=42
    )

    pipeline = Pipeline(stages=[imputer, indexer, encoder, assembler, scaler, rf_model])
    trained_model = pipeline.fit(train_df)

    predictions = trained_model.transform(test_df)
    print("\n🔍 Muestra de Predicciones vs Valores Reales (Test Set):")
    predictions.select(
        "trip_distance",
        "trip_duration_min",
        "fare_amount",
        F.round("prediction", 2).alias("predicted_fare_usd")
    ).show(10)

    # Evaluación de métricas formales (RMSE, MAE, R2)
    eval_rmse = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="rmse")
    eval_mae = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="mae")
    eval_r2 = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="r2")

    rmse_val = eval_rmse.evaluate(predictions)
    mae_val = eval_mae.evaluate(predictions)
    r2_val = eval_r2.evaluate(predictions)

    print("=================================================================")
    print("MÉTRICAS DE RENDIMIENTO DEL MODELO PREDICTIVO (PYSPARK.ML)")
    print("=================================================================")
    print(f"RMSE (Raíz del Error Cuadrático Medio): {rmse_val:.4f} USD")
    print(f"MAE  (Error Absoluto Medio):            {mae_val:.4f} USD")
    print(f"R²   (Coeficiente de Determinación):    {r2_val:.4f} ({r2_val*100:.2f}%)")
    print("=================================================================")


if __name__ == "__main__":
    s3_client_obj, bucket_name, spark_session = init_aws_and_spark()
    df_sampled = ingest_raw_data(s3_client_obj, bucket_name, spark_session)
    curated_dir = process_and_partition_etl(df_sampled, s3_client_obj, bucket_name)
    df_curated_data = run_descriptive_analytics(spark_session, curated_dir)
    run_predictive_model(df_curated_data)
