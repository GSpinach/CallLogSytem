package Cluster.CallLog.group.shuffle;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * App
 */
public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout
        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(2);
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt(),1).shuffleGrouping("wcspout").setNumTasks(1);
        //设置counter-Bolt
        builder.setBolt("counter-1", new CountBolt(),1).shuffleGrouping("split-bolt").setNumTasks(1);
        builder.setBolt("counter-2", new CountBolt(),3).fieldsGrouping("counter-1",new Fields("word")).setNumTasks(3);

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
