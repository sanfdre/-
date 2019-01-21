package my.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class LCountBlot extends BaseRichBolt {
    OutputCollector collector;
    Map<String,Integer> map = new HashMap<String, Integer>();
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        System.out.println("word:"+word);
        Integer nums = map.get(word);
        if (nums==null){
            nums = 1;
        }else {
            nums+=num;
        }
        map.put(word,nums);
        System.out.println("count :"+map);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
