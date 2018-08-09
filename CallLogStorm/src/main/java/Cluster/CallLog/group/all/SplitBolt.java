package Cluster.CallLog.group.all;

import Cluster.CallLog.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 */
public class SplitBolt implements IRichBolt {

    private TopologyContext context ;
    private OutputCollector collector ;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context ;
        this.collector = collector ;
    }

    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        Util.sendToLocalhost(this,line);
        String[] arr = line.split(" ");
        for(String s : arr){
            collector.emit(new Values(s,1));
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
