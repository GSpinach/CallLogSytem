package Cluster.CallLog.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * App
 */
public class App
{
    public static void main(String[] args) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout
        builder.setSpout("wcspout", new WordCountSpout(), 3).setNumTasks(3);
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt(), 4).shuffleGrouping("wcspout").setNumTasks(4);
        //设置counter-Bolt
        builder.setBolt("counter-bolt", new CountBolt(), 5).fieldsGrouping("split-bolt", new Fields("word")).setNumTasks(5);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式
         */
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("wc", conf, builder.createTopology());
//        Thread.sleep(10000);
        StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());
//        cluster.shutdown();

    }
}
