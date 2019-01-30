package kafka.my;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducer {

    public static void main(String[] args) {
        String topicName = "test";
        Properties props = new Properties();
        props.put("metadata.broker.list", "mini1:9092,mini1:9093,mini1:9094");
        props.put("request.required.acks", "1");

        Producer producer = new Producer(new ProducerConfig(props));

        for (int messageNo = 1;messageNo<10000;messageNo++){
            producer.send(new KeyedMessage(topicName,messageNo+"","appid:"+ UUID.randomUUID()+".sanfdre"));
        }
    }

}
