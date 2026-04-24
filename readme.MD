# 주제
- kafka

# repo
- https://github.com/edu-kenshi/kafka-local.git

# 설치
```
    docker compose up -d
```

# 패키지 설치
```
pip install kafka-python
```

# CLI 모드로 테스트
```
# 접속
# 호스트  PC
PS ~\kafka-local> docker container exec -it kafka bash

# 컨테이너
root@4527e52ba5f6:/# pwd
/
root@4527e52ba5f6:/# cd /opt/kafka/bin

# 토픽 생성
# 파티션 1개로 관리, 복제본 1개로 관리되는 토픽 구성
> kafka-topics.sh --create --topic spacex --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

# 토픽 전송(프로듀서)
> kafka-console-producer.sh --topic spacex --bootstrap-server 127.0.0.1:9092

# 컨슈머
> kafka-console-consumer.sh --topic spacex --bootstrap-server 127.0.0.1:9092

```

# local-datapipline 목표 시스템
```
[ LAYER ]          [ COMPONENT ]              [ PROTOCOL / ACTION ]             [ DATA FORMAT ]
====================================================================================================
SOURCE       :     Python (log_gen.py)  ----( Write to File )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                            ( Shared Volume )
               v                                                                   |
INGESTION    :     Fluent Bit (Agent)   <---( Tailing File  )----------------------+
               |          |
               |          | [ CHANNEL A: DIRECT BYPASS ]
               |          +--------------------------------------------( Produce )--+
               |          |                                                         |
               |          | [ CHANNEL B: PROXY ROUTE  ]                             |
               |          +----( Forward )----> [ Logstash ] ----( Produce )--+     |
               |                                     |                        |     |
               v                                     v                        v     v
TRANSFORM    : (A: Bypass)  +--[ Grok    : Pattern Matching & Extract Fields   ]--+ |
               |            +--[ Mutate  : Data Type Conversion (Str->Float)   ]--+ |
               |            +--[ Tagging : Add Metadata for Conditional Alerts ]--+ |
               |                                                              |     |
MESSAGING    :     Kafka / MSK (Broker) <-------------------------------------+-----+
               |          |             ( Merge Structured & Processed Data )
               |          |
               |          +----( Topic A: factory-json-topic ) <--- [Channel A Result]
               |          +----( Topic B: factory-text-topic ) <--- [Channel B Result]
               v                                                               |
VISUALIZE    :     OpenSearch (DB)      <---( Indexing & Search )---->  DASHBOARD (Kibana)
====================================================================================================
```
```
[ 계층 ]            [ 구성 요소 ]              [ 프로토콜 / 동작 ]              [ 데이터 형식 ]
====================================================================================================
데이터 소스  :     파이썬 (log_gen.py)  ----( 파일 쓰기 동작 )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                            ( 공유 볼륨 연결 )
               v                                                                   |
데이터 수집  :     Fluent Bit (에이전트) <---( 로그 실시간 추적 )----------------------+
               |          |
               |          | [ 채널 A: 최단거리 직결(json 데이터)]
               |          +--------------------------------------------( 메시지 발행 )--+
               |          |                                                          |
               |          | [ 채널 B: 가공 처리 중계(text 데이터)]                       |
               |          +----( 데이터 전달 )----> [ Logstash ] ----( 메시지 발행)--+     |
               |                                       |                          |  |
               v                                       v                          v  v
데이터 가공  : (A: 통과)    +--[ Grok    : 패턴 매칭을 통한 의미 있는 필드 추출 ]---+       |
               |            +--[ Mutate  : 데이터 타입 변환 (문자열 -> 실수형)  ]---+    |
               |            +--[ Tagging : 조건별 알람 처리를 위한 메타데이터  ]---+      
               |                                                                  |  |
데이터 적재  :     Kafka / MSK (브로커(허브))   <------------------------------------------+--+
               |          |             ( 직결 데이터와 가공 데이터 통합 수신 )
               |          |
               |          +----( 토픽 A: factory-json-topic ) <--- [채널 A 결과물]
               |          +----( 토픽 B: factory-text-topic ) <--- [채널 B 결과물]
               v                                                                  |
시각화/분석  :     OpenSearch (DB)      <---( 인덱싱 및 검색 )------>  대시보드 (Kibana)
====================================================================================================
```

# 프로젝트 재구성
- docker-compose.yml 수정(KRaft 적용)

```
    docker compose down
    docker compose up -d
```

# Fluentd(40~60MB) <-> Fluent-Bit(1~5MB)
- 태그 기반 라우팅 도구(Tag Based Routing)
- C 기반 작성
- fluent-bit/etc/fluent-bit.conf 에서 구성 기술 완료
- 목표
    - 데이터 수집, 처리, 전달 => ETL 도구
    - 경량형 오픈 소스 로그 프로세서
    - 고가용성, 고성능 처리, 데이터 처리 안정성
- 파이프라인
    - Input : 파일, 시스템 매트릭, http 스트림, ... 다양한 소스 제공
    - Output, Service, Parser, Filter, Buffer, Routing
    - Output : Kafka/Opensearch/Elasticsearch/s3/... 모두 가능함

- config 구성 -> 규칙 
- 지원 플로그인
    - 1000개(fluentd) <-> 100개(fluent-bit)