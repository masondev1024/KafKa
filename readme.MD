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
[ LAYER ]          [ COMPONENT ]          [ PROTOCOL / ACTION ]            [ DATA FORMAT ]
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
               v                                                               |
                                                                        kafka connect / ui
                                                                                |
                                                                                v               
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
               |          | [ 채널 A: 최단거리 직결 ]
               |          +--------------------------------------------( 메시지 발행 )--+
               |          |                                                          |
               |          | [ 채널 B: 가공 처리 중계 ]                                    |
               |          +----( 데이터 전달 )----> [ Logstash ] ----( 메시지 발행)--+     |
               |                                       |                          |  |
               v                                       v                          v  v
데이터 가공  : (A: 통과)    +--[ Grok    : 패턴 매칭을 통한 의미 있는 필드 추출 ]---+       |
               |            +--[ Mutate  : 데이터 타입 변환 (문자열 -> 실수형)  ]---+  |
               |            +--[ Tagging : 조건별 알람 처리를 위한 메타데이터  ]---+    |
               |                                                                  |  |
데이터 적재  :     Kafka / MSK (허브)   <------------------------------------------+--+
               |          |             ( 직결 데이터와 가공 데이터 통합 수신 )
               |          |
               |          +----( 토픽 A: factory-json-topic ) <--- [채널 A 결과물]
               |          +----( 토픽 B: factory-text-topic ) <--- [채널 B 결과물]
               v                                                                  |
                                                                            kafka connect / ui
                                                                                  |
                                                                                  v
시각화/분석  :     OpenSearch (DB)      <---( 인덱싱 및 검색 )------>  대시보드 (Kibana)
====================================================================================================
```


# 프로젝트 재구성
- docker-compose.yml 수정 (KRaft 적용된)
```
    docker compose down
    docker compose up -d
```

# Fluentd(40~60MB) <-> Fluent-Bit (1~5MB)
- 태그 기반 라우팅 도구(Tag Based Routing)
- Ruby(C 결합) 구조 <-> C 
- fluent-bit/etc/fluent-bit.conf 에서 구성 기술 완료
- 목표
    - 데이터 수집, 처리, 전달 => ETL 도구
    - 경량형 오픈 소스 로그 프로세서
    - 고가용성, 고성능 처리, 데이터 처리 안정성
- 파이프라인 
    - Input : 파일, 시스템 매트릭, http 스트림, .. 다양한 소스 제공
    - Service, Parser, Filter, Buffer, Rounting
    - Output : Kafka/Opensearch/Elasticsearch/s3/... 모두 가능함
- 지원 플러그인 
    - 1000여개 <-> 100여개

- conf 구성 => 규칙 기술
    ```
    docker compose restart
    ```

- 커슈머 구동
    ```
        PS ...> docker exec -it kafka-local bash

        # json 관련 토픽 커슈머
        > ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic factory-json-topic --from-beginning

        # text 관련 토픽 커슈머
        > ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic factory-text-topic --from-beginning
    ```

- 로그 발생 -> 카프카 컨슈머 수시 확인
    ```
        python log_gen.py
    ```
    
- conf가 수정되면
    - `docker compose restart` 반드시 수행

- 전달 내용
    - http로 전달해서 데이터가 늘어났음, 
    - forward등 전달 방식은 다양함
        - forward 시도, 24224 포트 사용
```
{
    "event":{
        "original":"[{\"date\":1777005819.42045,\"log\":\"\\\"[2026-04-24 13:43:38] ID=AI-FACTORY-001 |   TEMP:87.5 |   HUMI:42.1 |   STAT:RUNNING\\\"\"}]"},
        "host":{"ip":"172.19.0.3"},
        "user_agent":{"original":"Fluent-Bit"},
        "date":1.77700581942045E9,
        "url":{
            "path":"/",
            "domain":"logstash",
            "port":5044
        },
        "http":{
            "version":"HTTP/1.1",
            "method":"POST",
            "request":{
                "body":{
                    "bytes":"124"
                },
                "mime_type":"application/json"
            }
        },
        "log":"\"[2026-04-24 13:43:38] ID=AI-FACTORY-001 |   TEMP:87.5 |   HUMI:42.1 |   STAT:RUNNING\"","@version":"1",
        "@timestamp":"2026-04-24T04:43:40.412003029Z"
}

```

# 로그 행태
- 참고
    - 최초 데이터 => fluent-bit/logstash => 데이터 형태가 노이즈가 많음(프로토콜때문에 추가된 내용이 많음) => 필터역할 필요
    - 필터 보정하는 내용 업데이트(추후)

# kafka 대시보드
- kafka ui
- message 흐름, connect를 통해 전달이 잘되는지 등 모니터링

# local -> cloud 연결
- kafka connect