package Cluster.CallLog.group.shuffle;

import Cluster.CallLog.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * countbolt，使用二次聚合，解决数据倾斜问题。
 * 一次聚合和二次聚合使用field分组，完成数据的最终统计。
 * 一次聚合和上次split工作使用
 */
public class CountBolt implements IRichBolt{

    private Map<String,Integer> map ;

    private TopologyContext context;
    private OutputCollector collector;

    private long lastEmitTime = 0 ;

    private long duration = 5000 ;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        map = new HashMap<String, Integer>();
        map = Collections.synchronizedMap(map);
        //分线程，执行清分工作,线程安全问题
        Thread t = new Thread(){
            public void run() {
                while(true){
                    emitData();
                }
            }
        };
        //守护进程
        t.setDaemon(true);
        t.start();
    }

    private void emitData(){
        //清分map
        synchronized (map){
            //判断是否符合清分的条件
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                //向下一环节发送数据
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            //清空map
            map.clear();
        }
        //休眠
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        //提取单词
        String word = tuple.getString(0);
        Util.sendToLocalhost(this, word);
        //提取单词个数
        Integer count = tuple.getInteger(1);
        if(!map.containsKey(word)){
            map.put(word, count);
        }
        else{
            map.put(word,map.get(word) + count);
        }

    }

    public void cleanup() {
        for(Map.Entry<String,Integer> entry : map.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
