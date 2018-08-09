package Cluster.CallLog.util.hiveQuery;

import Cluster.CallLog.util.hbaseQuery.CallLog;

import java.util.List;

public class CallLogQuerySQL
{
    public static void main(String[] args)
    {
        CalllLogService cs = new CalllLogService();
        /**
         * 查询最近通话记录
         */
        CallLog log = cs.findLatestCallLog("15778423030");
        System.out.println(log);
        /**
         * 查询指定人员指定年份中各个月份的通话次数
         */
        List<CallLogStat> list = cs.statCallLogsCount("15778423030", "2018");
        for (CallLogStat cls : list)
        {
            System.out.println(cls);
        }
    }
}
