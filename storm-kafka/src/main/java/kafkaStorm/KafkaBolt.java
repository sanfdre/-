package kafkaStorm;


import com.google.gson.Gson;
import order.OrderInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

public class KafkaBolt extends BaseRichBolt {
    private JedisPool pool;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxTotal(1000*100);
        jedisPoolConfig.setMaxWaitMillis(50);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);

        pool = new JedisPool(jedisPoolConfig,"127.0.1",6379);
    }

    @Override
    public void execute(Tuple input) {
        String  message = new String((byte[]) input.getValue(0));
        OrderInfo orderInfo = new Gson().fromJson(message,OrderInfo.class);
        Jedis jedis = pool.getResource();
        jedis.incrBy("totalAmount",orderInfo.getProductPrice());
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
