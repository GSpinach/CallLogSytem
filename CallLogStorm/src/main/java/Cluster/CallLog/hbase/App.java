package Cluster.CallLog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * App
 */
public class App
{
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";

    public static void main(String[] args) throws Exception
    {
        //hbase映射
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")                //rowkey
                .withColumnFields(new Fields("word"))   //column
                .withCounterFields(new Fields("count")) //column
                .withColumnFamily("f1");                //列族

        Configuration hbaseConf = HBaseConfiguration.create();
        HBaseBolt hbaseBolt = new HBaseBolt("ns1:wordcount", mapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(1);
        builder.setBolt("split-bolt", new SplitBolt(), 2).shuffleGrouping("wcspout").setNumTasks(2);
        builder.setBolt("hbase-bolt", hbaseBolt, 2).fieldsGrouping("split-bolt", new Fields("word")).setNumTasks(2);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());

    }
}
