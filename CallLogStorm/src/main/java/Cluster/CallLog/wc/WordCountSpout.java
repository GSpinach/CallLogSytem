package Cluster.CallLog.wc;

import Cluster.CallLog.util.Util;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Spout
 */
public class WordCountSpout implements IRichSpout{
    private TopologyContext context ;
    private SpoutOutputCollector collector ;

    private List<String> states ;

    private Random r = new Random();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Util.sendToClient(this,"open()",7777);
        this.context = context ;
        this.collector = collector ;
        states = new ArrayList<String>();
        states.add("hello world tom");
        states.add("hello world tomas");
        states.add("hello world tomasLee");
        states.add("hello world tomson");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        Util.sendToClient(this, "nextTuple()",7777);
        String line = states.get(r.nextInt(4));
        collector.emit(new Values(line));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
