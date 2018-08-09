package Cluster.CallLog.group.direct;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * App
 */
public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout
        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(2);
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt(),2).directGrouping("wcspout").setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());
        //Thread.sleep(20000);
//        StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());
        //cluster.shutdown();

    }
}
