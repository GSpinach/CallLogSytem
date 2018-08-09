package Cluster.CallLog.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * Created by Administrator on 2017/4/3.
 */
public class App {
    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("ss");
        builder.addBolt(new AddBolt(), 3);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();

        cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

        System.out.println("Results for 'hello':" + drpc.execute("ss", "hello"));

        cluster.shutdown();
        drpc.shutdown();
    }

}
