package my.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class LWordCountTopologMain {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mySpot",new Lspot(),2);
        builder.setBolt("mySplit",new LSplitBolt(),2).shuffleGrouping("mySpot");
        builder.setBolt("myCount",new LCountBlot(),4).shuffleGrouping("mySplit");

        Config config = new Config();
        config.setNumWorkers(2);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount",config,builder.createTopology());
    }
}
