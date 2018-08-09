package Cluster.CallLog.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * 通话记录计数器Bolt
 */
public class CallLogCounterBolt implements IRichBolt{

    Map<String, Integer> counterMap;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String call = tuple.getString(0);
        Integer duration = tuple.getInteger(1);

        if (!counterMap.containsKey(call)) {
            counterMap.put(call, 1);
        } else {
            Integer c = counterMap.get(call) + 1;
            counterMap.put(call, c);
        }
        collector.ack(tuple);
    }

    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
