from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, lit, current_timestamp, min, max
from pyspark.sql.window import Window
from datetime import datetime
import sys
import os
import traceback


# -----------------------------
# 1) Spark Session 생성
# -----------------------------
def create_spark_session(app_name="Session Behavior Analysis"):
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .config("spark.jars", "/opt/spark/extra-jars/postgresql-42.7.1.jar")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.cores", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark 세션 생성 완료")
        return spark

    except Exception as e:
        print(f"❌ Spark 세션 생성 실패: {e}")
        raise


# -----------------------------
# 2) PostgreSQL 데이터 읽기
# -----------------------------
def read_clickstream_data(spark, start_date, end_date):

    jdbc_url = "jdbc:postgresql://postgres:5432/clickdb"
    connection_properties = {
        "user":  os.getenv("DB_USER"),
        "password":  os.getenv("DB_USER"),
        "driver": "org.postgresql.Driver"
    }

    # 날짜 형식 판단
    if "T" in start_date or " " in start_date:
        start_ts = start_date
        end_ts = end_date
    else:
        start_ts = f"{start_date} 00:00:00"
        end_ts = f"{end_date} 23:59:59"

    print(f"📥 PostgreSQL에서 Clickstream Data 읽는 중... ({start_ts} ~ {end_ts})")

    try:
        # 1) 전체 테이블을 로드
        df = spark.read.jdbc(
            url=jdbc_url,
            table="user_clickstream",
            properties=connection_properties
        )

        print(f"➡️ 전체 Row 수: {df.count():,}건")

        # 2) 날짜 조건 필터링 
        df = df.filter(
            (col("event_time") >= start_ts) &
            (col("event_time") < end_ts)
        )

        # 3) product_id 없는 데이터 제외
        df = df.filter(col("product_id").isNotNull())

        print(f"📉 기간 필터링 후 Row 수: {df.count():,}건")

        # 4) 필요한 컬럼만 선택
        df = df.select(
            "event_time",
            "event_type",
            "product_id",
            "category_code",
            "user_id",
            "user_session"
        )

        print(f"📦 최종 반환 컬럼 수: {len(df.columns)}개")
        return df

    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        import traceback
        traceback.print_exc()
        raise

# -----------------------------
# 3) 세션 행동 분석 수행
# -----------------------------
def analyze_sessions(df):

    print("🔍 세션 행동 분석 시작...")

    df = df.cache()

    # 세션 시작/종료 시간 계산
    session_time_df = df.groupBy("user_session").agg(
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end")
    ).withColumn(
        "session_length_sec",
        (col("session_end").cast("long") - col("session_start").cast("long"))
    )

    # 세션별 이벤트 count(view/cart/purchase)
    event_count_df = df.groupBy("user_session").pivot("event_type").count().fillna(0)

    # 세션 기반 전환 여부 (purchase 1개라도 있으면 전환됨)
    conversion_df = event_count_df.withColumn(
        "converted",
        F.when(col("purchase") > 0, 1).otherwise(0)
    )

    # 세션 관심 카테고리 (가장 많이 조회된 category_code)
    category_df = df.groupBy("user_session", "category_code").count()
    w = Window.partitionBy("user_session").orderBy(F.col("count").desc())

    category_ranked = category_df.withColumn(
        "rank", F.row_number().over(w)
    ).filter("rank = 1").withColumnRenamed("category_code", "interest_category")

    #결과 병합
    result = (
        session_time_df
        .join(conversion_df, "user_session", "left")
        .join(category_ranked.select("user_session", "interest_category"), "user_session", "left")
    )

    return result


# -----------------------------
# 4) 결과 저장
# -----------------------------
def save_session_behavior(result_df, date_str):

    jdbc_url = "jdbc:postgresql://postgres:5432/clickdb"
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    save_df = result_df.withColumn("date", F.to_date(lit(date_str))) \
                       .withColumn("created_at", current_timestamp())

    print("💾 결과 저장 (session_behavior_daily)")

    save_df.write.jdbc(
        url=jdbc_url,
        table="session_behavior_daily",
        mode="append",
        properties=connection_properties
    )

    print(f"✅ {date_str} 세션 행동 분석 저장 완료!")


# -----------------------------
# 5) 실행 함수
# -----------------------------
def run_session_behavior(date_str):
    print(f"\n==============================")
    print(f"📅 Session Behavior 분석 실행: {date_str}")
    print(f"==============================\n")

    spark = None

    try:
        spark = create_spark_session(f"Session Behavior - {date_str}")
        df = read_clickstream_data(spark, date_str, date_str)

        if df.count() == 0:
            print(f"⚠️ {date_str} 데이터 없음.")
            return

        result_df = analyze_sessions(df)

        print("\n📊 분석 결과 예시:")
        result_df.show(20, truncate=False)

        save_session_behavior(result_df, date_str)

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        traceback.print_exc()
        raise

    finally:
        if spark:
            spark.stop()


# -----------------------------
# 6) 직접 실행 코드
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python spark_session_behavior_job.py YYYY-MM-DD")
        sys.exit(1)

    date_str = sys.argv[1]
    run_session_behavior(date_str)
