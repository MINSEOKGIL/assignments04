
import os
import csv
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import re


# ✅ 로거 설정
def setup_logger():
    logger = logging.getLogger("StreamingProducer")
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    handler = logging.FileHandler("logs/producer.log", mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)
        logger.addHandler(console_handler)

    return logger


logger = setup_logger()






# ✅ Kafka Producer 생성
def create_safe_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093"),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks='all',
            #전송 중복(재시도 중복)은 방지하지만 데이터 내용 중복은 방지하지 않음.
            enable_idempotence=True,
        
            max_in_flight_requests_per_connection=1,  # idempotence 보장 
        )
        logger.info("✅ Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logger.exception(f"❌ Failed to initialize Kafka producer: {e}")
        raise


# ✅ Kafka Connect schema
schema = {
    "type": "struct",
    "fields": [
        {"type": "string", "optional": True, "field": "event_time"},
        {"type": "string", "optional": True, "field": "event_type"},
        {"type": "int64", "optional": True, "field": "product_id"},
        {"type": "int64", "optional": True, "field": "category_id"},
        {"type": "string", "optional": True, "field": "category_code"},
        {"type": "string", "optional": True, "field": "brand"},
        {"type": "float", "optional": True, "field": "price"},
        {"type": "int64", "optional": True, "field": "user_id"},
        {"type": "string", "optional": True, "field": "user_session"}
    ],
    "optional": False,
    "name": "user_clickstream_schema"
}


# ✅ 개별 CSV 파일 전송
def stream_csv_file_safe(file_path, topic, producer):
    print(f"\n{'='*80}")
    print(f"📂 파일 전송 시작: {os.path.basename(file_path)}")
    print(f"{'='*80}\n")
    logger.info(f"📂 Start streaming file: {file_path}")

    file_name = os.path.basename(file_path)
    failed_file = f"failed/{file_name}.failed.jsonl"
    os.makedirs("failed", exist_ok=True)

    sent_count = 0
    failed_count = 0
    start_time = time.time()

    def on_error(excp, row=None):
        nonlocal failed_count
        failed_count += 1
        logger.error(f"❌ Kafka delivery failed: {excp}")
        if row:
            with open(failed_file, 'a', encoding='utf-8') as ff:
                ff.write(json.dumps(row) + '\n')

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for line_num, row in enumerate(reader, start=1):
                try:
                    event_time_raw = row.get("event_time", "")
                    event_time_clean = event_time_raw.replace(" UTC", "").strip() 

                    event = {
                        # "event_time": event_time_clean,  # ← "2019-10-01 00:00:00"
                        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "event_type": row.get("event_type"),
                        "product_id": int(float(row["product_id"])) if row.get("product_id") else None,
                        "category_id": int(float(row["category_id"])) if row.get("category_id") else None,
                        "category_code": row.get("category_code"),
                        "brand": row.get("brand"),
                        "price": float(row["price"]) if row.get("price") else None,
                        "user_id": int(float(row["user_id"])) if row.get("user_id") else None,
                        "user_session": row.get("user_session"),
                    }

                    message = {"schema": schema, "payload": event}
                    product_key = event.get("product_id")

                    producer.send(topic, key=product_key, value=message).add_errback(on_error, row=row)
                    sent_count += 1

                    time.sleep(0.001)  # 초당 약 1000건 속도로 제한

                    if sent_count % 10000 == 0:
                        print(f"📤 [{datetime.now().strftime('%H:%M:%S')}] {sent_count:,}건 전송 완료")

                    if sent_count % 30000 == 0:
                        producer.flush()
                        logger.info(f"Chunk 완료 — 누적 전송: {sent_count:,}건")

                except (KeyError, ValueError) as e:
                    failed_count += 1
                    if failed_count <= 5:
                        print(f"⚠️ 데이터 오류 (라인 {line_num}): {e}")
                    logger.warning(f"⚠️ Data error on line {line_num}: {e}")
                    with open(failed_file, 'a', encoding='utf-8') as ff:
                        ff.write(json.dumps(row) + '\n')

                except Exception as e:
                    failed_count += 1
                    print(f"❌ 예상치 못한 에러 (라인 {line_num}): {e}")
                    logger.exception(f"❌ Unexpected error on line {line_num}: {e}")
                    with open(failed_file, 'a', encoding='utf-8') as ff:
                        ff.write(json.dumps(row) + '\n')

        producer.flush()

        print(f"\n{'='*80}")
        print(f"✅ 파일 전송 완료: {file_name}")
        print(f"   - 총 전송: {sent_count:,}개")
        print(f"   - 실패: {failed_count}개")
        print(f"{'='*80}\n")

        logger.info(f"✅ Completed {file_name}: {sent_count:,} sent, {failed_count} failed")

    except Exception as e:
        logger.exception(f"❌ Fatal error processing {file_path}: {e}")
        raise


# ✅ 폴더 내 모든 CSV 파일 순차 전송
def stream_all_csv(csv_dir, topic):
    if not os.path.exists(csv_dir):
        logger.error(f"❌ Directory not found: {csv_dir}")
        return

    producer = create_safe_producer()

    try:
        files = sorted([f for f in os.listdir(csv_dir) if f.endswith(".csv")], key=lambda x: x)

        print(f"\n🎬 전송 시작!")
        print(f"📁 디렉토리: {csv_dir}")
        print(f"📊 총 파일 수: {len(files)}개")
        print(f"🎯 토픽: {topic}\n")
        logger.info(f"📁 Found {len(files)} CSV files in {csv_dir}")

        for idx, file_name in enumerate(files, 1):
            print(f"\n[{idx}/{len(files)}] 파일 처리 중...")
            file_path = os.path.join(csv_dir, file_name)
            stream_csv_file_safe(file_path, topic, producer)

        print(f"\n🎉 모든 파일 전송 완료! 총 {len(files)}개 파일 처리됨\n")
        logger.info("🎯 All CSV files processed successfully.")

    except KeyboardInterrupt:
        print("\n⚠️ Ctrl+C 감지 → 안전 종료 중...")
        logger.warning("⚠️ Interrupted by Ctrl+C during CSV streaming")
    
    
    
    except Exception as e:
        logger.exception(f"❌ Error while processing CSV directory: {e}")
   
    finally:
        try:

            producer.flush()        # 🔥 무조건 실행
            producer.close(timeout=30)
        
            print("🔒 Kafka Producer 연결 종료")
            logger.info("🔒 Producer closed safely")
        except Exception as e:
            logger.error(f"⚠️ Failed to close producer: {e}")


# ✅ 메인 실행
if __name__ == "__main__":
    CSV_DIR = os.getenv("CSV_DIR", "/mnt/c/archive")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "user_clickstream")

    try:
        stream_all_csv(CSV_DIR, TOPIC_NAME)
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
        logger.info("⚠️ Interrupted by user")
    except Exception as e:
        logger.exception(f"❌ Producer terminated unexpectedly: {e}")
