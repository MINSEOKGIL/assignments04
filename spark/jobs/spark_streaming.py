from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, desc, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import logging, sys, os


# ✅ 로거 설정 함수
def setup_logger():
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("SparkViewConsumer")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    file_handler = logging.FileHandler("logs/spark_view_consumer.log", mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


logger = setup_logger()


#  Spark 세션 생성
def create_spark_session():
    try:
        spark = (
            SparkSession.builder
            .appName("KafkaViewConsumer")
            .master("spark://spark-master:7077")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        logger.info("✅ Spark session initialized successfully.")
        return spark
    except Exception as e:
        logger.exception(f"❌ Spark session initialization failed: {e}")
        raise


#  Kafka 메시지 구조 정의 (schema + payload)
payload_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])


#  메인 로직
def main():
    try:
        spark = create_spark_session()

        # Kafka에서 메시지 읽기
        df_raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093")
            .option("subscribe", "user_clickstream")
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 10000) 
            .load()
        )

       
        df_json = df_raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        df = df_json.select("data.payload.*")

        # 이벤트 시간 타입 변환 및 view 이벤트 필터링
        df = df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
        df = df.filter(col("event_type") == "view")

        #  Watermark 설정: 1분 지연된 데이터까지 허용
        df_watermarked = df.withWatermark("event_time", "1 minutes")
        
        #  Watermark 기반 상태 관리 중복 제거:
        # (user_session, product_id, event_time) 조합이 Watermark 기간 동안 고유함을 보장 (Exactly-Once)
        df_deduped = df_watermarked.dropDuplicates(
            ["user_session", "product_id", "event_time"]
        )

        # 윈도우 집계
        windowed_counts = (
            df_deduped
              .groupBy(
                  window(col("event_time"), "3 minutes"),
                  col("product_id")
              )
              .agg(count("*").alias("view_count"))
        )


        #  Top-N 정렬
        topn = (
            windowed_counts
            .orderBy(col("window.start").asc(), desc("view_count"))
            .limit(10)
        )

        #  콘솔 출력
        query = (
            topn.writeStream
            .outputMode("complete") 
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="30 seconds")
            .option("checkpointLocation", "checkpoint/dir")
            .start()
        )

        logger.info("🚀 Spark streaming started successfully with watermark.")
        query.awaitTermination()

    except Exception as e:
        logger.exception(f"❌ Streaming process failed: {e}")

    finally:
        try:
            spark.stop()
            logger.info("🛑 Spark session stopped safely.")
        except Exception as e:
            logger.warning(f"⚠️ Spark session stop failed: {e}")


if __name__ == "__main__":
    main()
