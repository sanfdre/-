package kafkaStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class KafkaBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String  message = new String((byte[]) input.getValue(0));
        System.out.println(message);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
