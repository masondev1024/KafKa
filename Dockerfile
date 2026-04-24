FROM confluentinc/cp-kafka-connect:7.5.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-aws-firehose:latest