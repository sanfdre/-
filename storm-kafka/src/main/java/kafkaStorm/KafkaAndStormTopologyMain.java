package kafkaStorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaAndStormTopologyMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String zkHosts = "mini1:2181";
        String topic = "test";
        String zkRoot = "/myKafka";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(
                new SpoutConfig(new ZkHosts(zkHosts), topic,
                        zkRoot, "kafkaSpout")),
                1);
        builder.setBolt("kafkaBolt",new KafkaBolt(),1).shuffleGrouping("kafkaSpout");

        Config config = new Config();
        config.setNumWorkers(1);


        //3、提交任务  -----两种模式 本地模式和集群模式
        if (args.length>0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafkaAndStorm", config, builder.createTopology());
        }
    }
}
