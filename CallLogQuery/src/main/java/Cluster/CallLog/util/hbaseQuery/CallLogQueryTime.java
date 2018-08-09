package Cluster.CallLog.util.hbaseQuery;

import java.util.List;

public class CallLogQueryTime
{
    public static void main(String[] args)
    {
        String call = "15151889601";//主叫号码
        String startTime = "20180220";
        String endTime = "20190117";
        List<CallLogRange> list = CallLogUtil.getCallLogRanges(startTime, endTime);
//        System.out.println(list);
        List<CallLog> logs = new CallLogService().findCallogs(call, list);
        System.out.println(logs);
    }
}
