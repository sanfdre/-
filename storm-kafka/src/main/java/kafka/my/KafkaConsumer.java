package kafka.my;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumer implements Runnable{
    private String title;

    private KafkaStream kafkaStream;

    public KafkaConsumer(String title,KafkaStream kafkaStream) {
        this.title = title;
        this.kafkaStream = kafkaStream;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("group.id", "sanfdre-consumer");
        props.put("zookeeper.connect", "mini1:2181");
        props.put("auto.offset.reset", "largest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        String topic = "test";
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,3);
        Map<String, List<KafkaStream<byte[], byte[]>>> kafkaStreamMap = consumerConn.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[],byte[]>> kafkaStreams = kafkaStreamMap.get(topic);

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        for (int i = 0;i<kafkaStreams.size();i++){
            executorService.execute(new KafkaConsumer("第"+i+"个",kafkaStreams.get(i)));
        }
    }

    @Override
    public void run() {
        System.out.println("开始运行："+title);
        ConsumerIterator<byte[],byte[]> consumerIterator = kafkaStream.iterator();

        while (consumerIterator.hasNext()){
            MessageAndMetadata<byte[],byte[]> messageAndMetadata =  consumerIterator.next();
            String topic = messageAndMetadata.topic();
            int partition = messageAndMetadata.partition();
            long offset = messageAndMetadata.offset();
            String message = new String(messageAndMetadata.message());
            System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, message));
        }
        System.out.println(String.format("Consumer: [%s] exiting ...", title));
    }
}
