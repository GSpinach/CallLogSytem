package Cluster.CallLog.util.coproessor;

import java.text.DecimalFormat;

public class CallLogUtil
{
    //格式化
    private static DecimalFormat df = new DecimalFormat();

    /**
     * 获取hash值，默认分区数100
     */
    public static String getHashcode(String caller, String callTime, int partitions)
    {
        int len = caller.length();
        //获取电话后四位
        String last4code = caller.substring(len - 4);
        //获取时间：年月
        String year_month = callTime.substring(0, 6);
        //计算hash值
        int hashcode = (Integer.parseInt(last4code) ^ Integer.parseInt(year_month)) % partitions;
        return df.format(hashcode);
    }
}
