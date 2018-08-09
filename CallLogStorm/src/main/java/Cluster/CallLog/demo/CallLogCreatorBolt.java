package Cluster.CallLog.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 创建CallLog日志的Bolt
 */
public class CallLogCreatorBolt implements IRichBolt {
    //
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector ;
    }

    public void execute(Tuple tuple) {
        //处理通话记录
        String from = tuple.getString(0);
        String to = tuple.getString(1);
        Integer duration = tuple.getInteger(2);
        //产生新的tuple
        collector.emit(new Values(from + " - " + to, duration));
    }

    public void cleanup() {

    }

    /**
     * 设置输出字段的名称
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call", "duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
