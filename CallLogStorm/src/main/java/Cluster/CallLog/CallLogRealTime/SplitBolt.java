package Cluster.CallLog.CallLogRealTime;

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
public class SplitBolt implements IRichBolt
{

    private TopologyContext context;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.context = context;
        this.collector = collector;
    }

    public void execute(Tuple tuple)
    {
        String line = tuple.getString(0);
        String[] arr = line.split(",");
        collector.emit(new Values(arr[0], arr[1], arr[2], arr[3]));
        System.out.println(arr[0] + " --- " + arr[1] + ",startTime: " + arr[2] + ",totalTime: " + arr[3]);
    }

    public void cleanup()
    {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("caller", "callee", "callTime", "callDuration"));

    }

    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
}
