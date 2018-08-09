package Cluster.CallLog.CallLogRealTime;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class App
{
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException
    {
        //从kafka中读取数据，至Spout
        TopologyBuilder builder = new TopologyBuilder();

        //zk连接串
        BrokerHosts hosts = new ZkHosts("centos6:2182");
        //Spout配置
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "CallLog", "/home/ghb/HadoopCluster/zookeeper/CallLog_Storm_ZKLog", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        //hbase映射存储
//        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
//                .withRowKeyField("CallLog")                //rowkey
//                .withColumnFields(new Fields("caller"))   //column
//                .withCounterFields(new Fields("callee")) //column
//                .withColumnFields(new Fields("callTime"))   //column
//                .withCounterFields(new Fields("callDuration")) //column
//                .withColumnFamily("f1");                //列族


        builder.setSpout("kafkaspout", kafkaSpout).setNumTasks(2);
        builder.setBolt("split-bolt", new SplitBolt(), 2).shuffleGrouping("kafkaspout").setNumTasks(2);
        builder.setBolt("hbase-bolt", new HbaseBolt(), 2).shuffleGrouping("split-bolt").setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CallLog", conf, builder.createTopology());
//        StormSubmitter.submitTopology("mytop", conf, builder.createTopology());


//        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(1);
//        builder.setBolt("split-bolt", new Cluster.CallLog.hbase.SplitBolt(), 2).shuffleGrouping("wcspout").setNumTasks(2);
//        builder.setBolt("hbase-bolt", hbaseBolt, 2).fieldsGrouping("split-bolt", new Fields("word")).setNumTasks(2);
//
//        Config conf = new Config();
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("wc", conf, builder.createTopology());
    }
}
