package Cluster.CallLog.group.custom;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义分组
 */
public class MyGrouping implements CustomStreamGrouping {

    //接受目标任务的id集合
    private List<Integer> targetTasks ;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks ;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> subTaskIds = new ArrayList<Integer>();
        for(int i = 0 ; i <= targetTasks.size() / 2 ; i ++){
            subTaskIds.add(targetTasks.get(i));
        }
        return subTaskIds;
    }
}
