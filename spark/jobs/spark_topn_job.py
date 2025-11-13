

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, row_number, lit, current_timestamp
from pyspark.sql.window import Window
from datetime import datetime
import sys


def create_spark_session(app_name="TopN Calculator"):

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/opt/spark/extra-jars/postgresql-42.7.1.jar") \
        .config("spark.sql.adaptive.enabled", "true") \                
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_clickstream_data(spark, start_date, end_date):
    
    jdbc_url = "jdbc:postgresql://postgres:5432/clickdb"
    connection_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    # 날짜인지, 시간까지 포함된 문자열인지 판단
    if "T" in start_date or " " in start_date:
        start_ts = start_date
        end_ts = end_date
    else:
        start_ts = f"{start_date} 00:00:00"
        end_ts = f"{end_date} 23:59:59"

    query = f"""
        (SELECT 
            event_time,
            event_type,
            product_id,
            category_id,
            category_code,
            brand,
            price,
            user_id,
            user_session
        FROM user_clickstream
        WHERE event_time >= '{start_ts}'::timestamp
          AND event_time < '{end_ts}'::timestamp
          AND event_type = 'view'
          AND product_id IS NOT NULL
        ) AS clickstream_data
    """

    print(f"📊 데이터 읽기: {start_ts} ~ {end_ts}")

    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    print(f"✅ 읽은 레코드 수: {df.count():,}건")
    return df


def calculate_topn(spark, df, top_n=10):
    print(f"🔢 TopN 계산 중... (상위 {top_n}개)")

    # JDBCRelation → InMemoryRelation (에러 방지)
    df = df.cache()

    product_counts = df.groupBy("product_id").agg(
        F.count("*").alias("view_count"),
        F.first("brand", ignorenulls=True).alias("brand_sample"),
        F.first("category_code", ignorenulls=True).alias("category_sample")
    )

    window_spec = Window.orderBy(col("view_count").desc())

    topn_df = product_counts \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= top_n) \
        .orderBy("rank")

    return topn_df


def save_daily_topn(topn_df, date_str):
    jdbc_url = "jdbc:postgresql://postgres:5432/clickdb"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"

        
    }

    result_df = topn_df \
        .withColumn("date", lit(date_str)) \
        .withColumn("created_at", current_timestamp()) \
        .select("date", "rank", "product_id", "view_count",
                "brand_sample", "category_sample", "created_at")

    print(f"💾 결과 저장 중... (daily_topn_results)")

    result_df.write.jdbc(
        url=jdbc_url,
        table="daily_topn_results",
        mode="append",
        properties=connection_properties
    )

    print(f"✅ {date_str} TopN 저장 완료!")


def save_period_topn(topn_df, start_date, end_date):
    jdbc_url = "jdbc:postgresql://postgres:5432/clickdb"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    result_df = topn_df \
        .withColumn("period_start", lit(start_date)) \
        .withColumn("period_end", lit(end_date)) \
        .withColumn("created_at", current_timestamp()) \
        .select("period_start", "period_end", "rank", "product_id",
                "view_count", "brand_sample", "category_sample", "created_at")

    print(f"💾 결과 저장 중... (period_topn_results)")

    result_df.write.jdbc(
        url=jdbc_url,
        table="period_topn_results",
        mode="append",
        properties=connection_properties
    )

    print(f"✅ {start_date} ~ {end_date} TopN 저장 완료!")


def run_daily_topn(date_str, top_n=10):
    print(f"\n{'='*60}")
    print(f"📅 Daily TopN 실행: {date_str}")
    print(f"{'='*60}\n")

    spark = None
    try:
        spark = create_spark_session(f"Daily TopN - {date_str}")
        df = read_clickstream_data(spark, date_str, date_str)

        if df.count() == 0:
            print(f"⚠️ {date_str}에 데이터가 없습니다.")
            return

        topn_df = calculate_topn(spark, df, top_n)

        print(f"\n🏆 {date_str} TopN 결과:")
        topn_df.select("rank", "product_id", "view_count",
                       "brand_sample", "category_sample").show(top_n, truncate=False)

        save_daily_topn(topn_df, date_str)
        print(f"\n✅ Daily TopN 완료!")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark:
            spark.stop()


def run_period_topn(start_date, end_date, top_n=10):
    print(f"\n{'='*60}")
    print(f"📅 Period TopN 실행: {start_date} ~ {end_date}")
    print(f"{'='*60}\n")

    spark = None
    try:
        spark = create_spark_session(f"Period TopN - {start_date} to {end_date}")
        df = read_clickstream_data(spark, start_date, end_date)

        if df.count() == 0:
            print(f"⚠️ {start_date} ~ {end_date} 기간에 데이터가 없습니다.")
            return

        topn_df = calculate_topn(spark, df, top_n)

        print(f"\n🏆 {start_date} ~ {end_date} TopN 결과:")
        topn_df.select("rank", "product_id", "view_count",
                       "brand_sample", "category_sample").show(top_n, truncate=False)

        save_period_topn(topn_df, start_date, end_date)
        print(f"\n✅ Period TopN 완료!")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법:")
        print("  일일 TopN: python spark_topn_job.py daily YYYY-MM-DD [top_n]")
        print("  기간 TopN: python spark_topn_job.py period YYYY-MM-DD YYYY-MM-DD [top_n]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "daily":
        date_str = sys.argv[2]
        top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        run_daily_topn(date_str, top_n)

    elif mode == "period":
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 10
        run_period_topn(start_date, end_date)

    else:
        print(f"❌ 알 수 없는 모드: {mode}")
        sys.exit(1)
