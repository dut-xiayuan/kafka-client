package xyytest;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;

import java.util.HashMap;
import java.util.Map;

public class KafkaComsumer {
  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();
    String topic = "topic-test-xyy";
    String boostrapServer = "10.24.101.89:9092";

    // creating the consumer using map config
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", boostrapServer);
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    // use consumer for interacting with Apache Kafka
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

    // subscribes to the topic
    consumer.subscribe(topic, ar -> {

      if (ar.succeeded()) {


        vertx.setPeriodic(1000, timerId -> {

          consumer.poll(100, ar1 -> {

            if (ar1.succeeded()) {

              KafkaConsumerRecords<String, String> records = ar1.result();

              System.out.println(records.size());

              for (int i = 0; i < records.size(); i++) {
                KafkaConsumerRecord<String, String> record = records.recordAt(i);
                System.out.println("key=" + record.key() + ",value=" + record.value() +
                  ",partition=" + record.partition() + ",offset=" + record.offset());
              }
            }
          });

        });
      }
    });





  }
}
