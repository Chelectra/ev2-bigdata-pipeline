-- Crear base de datos en el catálogo de AWS Glue / Athena
CREATE DATABASE IF NOT EXISTS db_ev2_bigdata;

-- Crear tabla externa apuntando a los archivos Parquet particionados en S3
CREATE EXTERNAL TABLE IF NOT EXISTS db_ev2_bigdata.nyc_trips_curated (
    VendorID INT,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    pickup_hour INT,
    pickup_day_of_week INT,
    trip_duration_min DOUBLE,
    tip_amount DOUBLE,
    fare_amount DOUBLE,
    total_amount DOUBLE
)
PARTITIONED BY (payment_type_str STRING)
STORED AS PARQUET
LOCATION 's3://ev2-bigdata-cayupel-aguila/curated/nyc_trips/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Cargar particiones automáticamente
MSCK REPAIR TABLE db_ev2_bigdata.nyc_trips_curated;

-- Consulta analítica descriptiva
SELECT 
    payment_type_str AS metodo_pago,
    COUNT(*) AS total_viajes,
    ROUND(AVG(trip_distance), 2) AS distancia_promedio_millas,
    ROUND(AVG(trip_duration_min), 2) AS duracion_promedio_min,
    ROUND(AVG(fare_amount), 2) AS tarifa_promedio_usd,
    ROUND(AVG(tip_amount), 2) AS propina_promedio_usd
FROM db_ev2_bigdata.nyc_trips_curated
GROUP BY payment_type_str
ORDER BY total_viajes DESC;
