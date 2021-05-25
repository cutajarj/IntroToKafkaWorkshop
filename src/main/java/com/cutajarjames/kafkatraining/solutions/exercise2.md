1. `kafka-configs.bat --bootstrap-server localhost:9092 --describe --topic kafkaTrainingCutajarj --all`
2. `kafka-configs.bat --bootstrap-server localhost:9092 --alter --add-config retention.ms=43200000 --topic kafkaTrainingCutajarj`
3. `kafka-topics.bat --bootstrap-server localhost:9092 --create --topic kafkaTrainingRepCutajarj --partitions 5 --replication-factor 2`