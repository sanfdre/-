package kafkaStorm;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;



public class KafkaAndStormTopologyMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        String zkHostsStr = "mini1:2181";
        ZkHosts zkHosts = new ZkHosts(zkHostsStr);
        String topic = "orderMq";
        String zkRoot = "/myKafka";

        SpoutConfig spoutConfig = new SpoutConfig(zkHosts,topic,zkRoot,"kafkaSpout");
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout,1);
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
