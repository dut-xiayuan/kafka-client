package xyytest;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducer {
  public static void main(String[] args) throws Exception {

    Vertx vertx = Vertx.vertx();
    String topic = "topic-test-xyy";
    String boostrapServer = "10.24.101.89:9092";

    // creating the producer using map and class types for key and value serializers/deserializers
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", boostrapServer);
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "1");

    // use producer for interacting with Apache Kafka
    io.vertx.kafka.client.producer.KafkaProducer<String, String> producer = io.vertx.kafka.client.producer.KafkaProducer.create(vertx, config);

    for (int i = 0; ; i++) {

      // only topic and message value are specified, round robin on destination partitions
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create(topic, "message_" + i);
      System.out.println("write " + record.value());

      Thread.sleep(200);

      producer.write(record, done -> {

        if (done.succeeded()) {

          RecordMetadata recordMetadata = done.result();
          System.out.println("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
            ", partition=" + recordMetadata.getPartition() +
            ", offset=" + recordMetadata.getOffset());
        }
      });
    }

  }
}
