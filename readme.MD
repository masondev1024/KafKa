# Kafka Streaming PoC

`local-datapipeline` 브랜치의 센서 로그 수집 실습을 기반으로, Kafka를 운영 가능한 데이터 파이프라인의 중심으로 확장한 로컬 PoC입니다.

## 목표

- 파일 로그 수집과 Kafka raw topic 발행
- JSON/text 입력을 하나의 versioned data contract로 정규화
- 유효하지 않은 이벤트를 DLQ로 격리
- at-least-once 처리와 명시적 offset commit
- `event_id` 기반 중복 방지
- 로컬 DuckDB sink를 통한 분석 데이터 제공
- AWS Firehose 전송 경로는 `aws` Compose profile로 분리

## 아키텍처

```text
log_gen.py
    ├── sensor_json.log ──> Fluent Bit ──> factory.sensor.raw.json.v1 ──┐
    └── sensor_text.log ──> Fluent Bit ──> Logstash ──> factory.sensor.raw.text.v1 ─┤
                                                                                      │
                                                        stream-processor             │
                                          normalize / validate / DLQ                 │
                                             ├── factory.sensor.clean.v1 ──> stream-sink ──> data/sensor.duckdb
                                             └── factory.sensor.dlq.v1
```

Kafka 내부 컨테이너는 `kafka:9092`, 호스트에서 접근할 때는 `localhost:29092`를 사용합니다. 로컬 브로커는 KRaft 단일 노드이며 replication factor는 1입니다. 이는 개발용 설정이고 고가용성 구성이 아닙니다.

## 실행

```bash
docker compose up -d --build
python3 log_gen.py
```

상태 확인:

```bash
docker compose ps
docker compose logs -f stream-processor stream-sink
```

토픽 확인:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

정규화된 이벤트 확인:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic factory.sensor.clean.v1 \
  --from-beginning
```

DLQ 확인:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic factory.sensor.dlq.v1 \
  --from-beginning
```

DuckDB 조회:

```bash
docker compose stop stream-sink
docker compose run --rm --no-deps stream-sink python -c \
  "import duckdb; print(duckdb.connect('/data/sensor.duckdb').execute('select sensor_id, count(*) as events from sensor_events group by sensor_id order by sensor_id').fetchall())"
docker compose start stream-sink
```

DuckDB 파일은 단일 writer lock을 사용하므로 sink가 실행 중인 동안 별도 프로세스에서
같은 파일을 열 수 없습니다. 운영 환경에서는 전용 OLAP 저장소나 Iceberg/Parquet 테이블을
조회 대상으로 분리합니다.

Kafka UI는 <http://localhost:8081>에서 확인할 수 있습니다.

## 데이터 계약

정규화된 `factory.sensor.clean.v1` 이벤트는 다음 필드를 보장합니다.

```json
{
  "schema_version": "factory-sensor.v1",
  "event_id": "sha256(...)",
  "event_time": "2026-08-23T12:00:00Z",
  "ingested_at": "2026-08-23T12:00:01Z",
  "sensor_id": "AI-FACTORY-001",
  "temperature": 87.5,
  "humidity": 42.4,
  "status": "RUNNING",
  "source": {
    "topic": "factory.sensor.raw.json.v1",
    "partition": 0,
    "offset": 12
  }
}
```

현재 검증 규칙은 다음과 같습니다.

- `sensor_id`: 영숫자, `_`, `-` 조합
- `temperature`: `-40` 이상 `150` 이하
- `humidity`: `0` 이상 `100` 이하
- `status`: `RUNNING`, `IDLE`, `STOPPED`, `ERROR`
- timestamp 필수

검증 실패 이벤트는 원문과 실패 원인, Kafka source offset을 포함해 `factory.sensor.dlq.v1`로 보냅니다.

## 장애·중복 처리 설계

- Producer는 `acks=all`, retry, gzip compression을 사용합니다.
- Processor는 output publish가 성공한 뒤에만 input offset을 commit합니다.
- publish 후 commit 전에 장애가 나면 동일 이벤트가 재처리될 수 있으므로 at-least-once semantics입니다.
- Sink는 `event_id` primary key와 `ON CONFLICT DO NOTHING`으로 중복 저장을 방지합니다.
- Fluent Bit는 파일 offset DB와 filesystem buffer를 사용해 재시작 시 재전송·유실 위험을 줄입니다.

## 테스트

```bash
python3 -m unittest discover -s tests -v
```

DLQ 경로를 강제로 확인하려면 별도 터미널에서 다음처럼 invalid event 비율을 높여 실행합니다.

```bash
INVALID_RATE=1 INTERVAL_SECONDS=1 python3 log_gen.py
```

## AWS 전송 경로

기본 실행에서는 AWS 자격증명을 요구하지 않습니다. Vector 기반 Firehose sink를 별도로 실행할 때만 환경변수를 설정합니다.

```bash
export AWS_ACCESS_KEY=...
export AWS_SECRET_KEY=...
export AWS_REGION=ap-northeast-2
docker compose --profile aws up -d vector
```

운영 환경에서는 장기 access key 대신 IAM role 또는 workload identity를 사용해야 합니다.

## 다음 고도화 단계

1. JSON Schema/Schema Registry와 backward-compatible schema evolution
2. Prometheus exporter 및 consumer lag/error-rate alerting
3. DuckDB sink를 Spark Structured Streaming + Parquet/Iceberg로 교체
4. Airflow DAG로 품질검사, compact, backfill 자동화
5. Kafka TLS/SASL, ACL, secret manager 적용
6. 단일 브로커를 다중 broker와 replication factor 3 환경으로 확장
7. Testcontainers 기반 통합 테스트와 장애 주입 테스트 추가
