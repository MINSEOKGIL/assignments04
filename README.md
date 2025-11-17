
# 복제본을 2개로 설정하여 브로커 하나가 장애가 나더라도  
# 다른 브로커가 자동으로 리더 승격되어 데이터 유실 없이 처리되도록 토픽수동생성
kafka-topics --delete --topic user_clickstream --bootstrap-server kafka1:9092
kafka-topics --create \
  --topic user_clickstream \
  --bootstrap-server kafka1:9092 \
  --replication-factor 2 \
  --partitions 3




#postgres cli접속
psql -U admin -d clickdb



# rawdata DB테이블 수동생성
CREATE TABLE user_clickstream (
    event_time TIMESTAMP NOT NULL,  -- ✅ TIMESTAMP 타입
    event_type VARCHAR(50),
    product_id BIGINT NOT NULL,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(100),
    price DOUBLE PRECISION,
    user_id BIGINT NOT NULL,
    user_session VARCHAR(255),
    
    PRIMARY KEY (product_id, user_id, event_time)
);




# API상태체크
curl http://localhost:8083/connectors


# 커넥터가 초기화됐을경우 재등록
# product_id, user_id, event_time event_type로  pirmarykey지정 이벤트의 유니크 KEY역할수행
# 이러한 key를 기준으로 데이터가 있으면 update, 없으면 insert
# POSTGRES DB 데이터 중복을 방지
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "clickstream-postgres-sink",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max": "2",
      "topics": "user_clickstream",
      "connection.url": "jdbc:postgresql://postgres:5432/clickdb",
      "connection.user": "admin",
      "connection.password": "admin",
      "auto.create": "false",
      "auto.evolve": "true",
      "insert.mode": "upsert",
      "pk.mode": "record_value",
      "pk.fields": "user_session,event_time",
      "transforms": "convertTS",
      "transforms.convertTS.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.convertTS.field": "event_time",
      "transforms.convertTS.format": "yyyy-MM-dd HH:mm:ss",
      "transforms.convertTS.target.type": "Timestamp",
      

      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "true"
    }
  }'

# 커넥터오류확인
docker logs kafka-connect | grep user_clickstream



# postgres 확인
docker exec -it postgres psql -U admin -d clickdb

SELECT COUNT(*) FROM "user_clickstream";


# 스파크 데일리 실행
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/postgresql-42.7.1.jar \
  /opt/spark/jobs/spark_topn_job.py daily 2019-10-01 10


# streaming.py실행명령어
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\
/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,\
/opt/spark/extra-jars/kafka-clients-3.5.1.jar,\
/opt/spark/extra-jars/commons-pool2-2.12.0.jar,\
/opt/spark/extra-jars/postgresql-42.7.1.jar \
  /opt/spark/jobs/spark_streaming.py