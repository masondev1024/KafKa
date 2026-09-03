# AWS POC validation snapshot

검증일: 2026-09-03
리전: `ap-northeast-2`
범위: Terraform으로 임시 생성한 S3/Firehose/Glue/CloudWatch/IAM과 로컬 Kafka → Vector 경로

계정 ID, bucket 이름, query execution ID 같은 환경 식별자는 공개 포트폴리오 문서에 남기지
않습니다. 아래 수치는 이번 일회성 실행에서 CLI와 worker metrics로 직접 측정한 값입니다.

## End-to-end 결과

| 경계 | 측정값 | 판정 |
| --- | ---: | --- |
| JSON raw 입력 | 35 records | 정상 |
| Processor clean | 29 records | 정상 |
| Processor DLQ | 6 records | 의도한 온도 범위 위반 격리 |
| Kafka processor lag | 0 | 정상 |
| DuckDB sink 저장 | 29 records | 정상 |
| Firehose IncomingRecords | 29 | 정상 |
| Firehose SucceedConversion.Records | 29 | 정상 |
| Firehose FailedConversion.Records | 0 | 정상 |
| Firehose DeliveryToS3.Records | 29 | 정상 |
| Firehose DeliveryToS3.Success | 1 | 정상 |
| Firehose ThrottledRecords | 0 | 정상 |
| Firehose DataFreshness | 65 seconds | 60초 buffering 설정과 일치 |
| S3 bronze output | 1 Parquet object / 5,710 bytes | 정상 |
| Athena landed rows | 29 | 정상 |
| Athena unique event_id | 29 | 중복 없음 |
| Athena invalid temperature/humidity/status | 0 / 0 / 0 | 정상 |
| Athena DataScannedInBytes | 2,687 | partition predicate 적용 |

S3 object는 `bronze/ingest_year=2026/ingest_month=09/ingest_day=03/` 아래에 생성되었습니다.
로컬 DuckDB로 Parquet을 재확인한 결과 `event_id` 29개가 모두 unique했고, `source` nested
struct와 `vector_ingest_at`, `pipeline` metadata가 보존되었습니다.

## Observability 결과

- Prometheus scrape targets: processor/sink 모두 `up`
- Processor lag gauges: 모든 assigned partition에서 `0`
- Sink lag: 모든 assigned partition에서 `0`
- Processor metrics: `clean=29`, `dlq=6`
- Sink metrics: `stored=29`, write failure metric 없음
- DLQ rate alert expression: 약 `10.35%`, `pending`

DLQ alert는 `for: 5m` 조건을 가지므로 짧은 synthetic burst에서 즉시 `firing`으로 바뀌지
않는 것이 의도된 동작입니다. 지속적인 오류율은 다음 운영 실습에서 장시간 입력으로 검증할
수 있습니다.

## 트러블슈팅 기록

1. `default` profile은 `InvalidClientTokenId`가 발생했습니다. 폐기된 access key를 재사용하지
   않고 `aws sso login --profile develope-test`로 SSO 세션을 갱신한 뒤
   `aws configure export-credentials --format env`의 단기 credential만 Docker 프로세스에
   주입해 해결했습니다. credential은 repository나 `.env`에 저장하지 않았습니다.
2. 첫 CloudWatch `DeliveryToS3.Records` 조회는 datapoint가 비어 있었습니다. Firehose와
   CloudWatch metric publication 지연을 고려해 `list-metrics`로 stream dimension을 확인하고
   재조회한 결과 `Incoming=29`, `Conversion=29`, `Delivery=29`가 확인되었습니다.
3. 이벤트 생성 후 카운트를 출력하는 shell에서 zsh 예약 변수 `status`를 사용해 후처리만
   실패했습니다. 생성된 파일에는 영향이 없었고 변수명을 `rc`로 바꿔 수치를 재확인했습니다.
4. 로컬 Kafka 재기동 직후 `NotCoordinatorError`가 일시적으로 반복됐습니다. consumer가
   coordinator를 재탐색하고 group에 재가입한 뒤 lag 0으로 수렴했으므로 데이터 손실로
   판단하지 않았습니다. 운영 Kafka에서는 broker HA와 coordinator 안정성을 별도 검증해야
   합니다.

## 정리

검증 후 Docker/Prometheus를 종료하고, versioning이 켜진 S3의 current/non-current
object와 Athena query result까지 삭제한 뒤 Terraform destroy를 실행합니다. 장기 실행을
전제로 하지 않는 포트폴리오 실습에서는 이 teardown을 반드시 수행합니다.
