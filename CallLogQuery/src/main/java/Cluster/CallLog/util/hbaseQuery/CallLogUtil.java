package Cluster.CallLog.util.hbaseQuery;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class CallLogUtil
{
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat sdfFriend = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    //格式化
    private static DecimalFormat df = new DecimalFormat();


    /**
     * 获取hash值,默认分区数100
     */
    public static String getHashcode(String caller, String callTime, int partitions)
    {
        int len = caller.length();
        //取出后四位电话号码
        String last4Code = caller.substring(len - 4);
        //取出时间单位,年份和月份.
        String mon = callTime.substring(0, 6);
        //
        int hashcode = (Integer.parseInt(mon) ^ Integer.parseInt(last4Code)) % partitions;
        return df.format(hashcode);
    }

    /**
     * 起始时间
     */
    public static String getStartRowkey(String caller, String startTime, int partitions)
    {
        String hashcode = getHashcode(caller, startTime, partitions);
        return hashcode + "," + caller + "," + startTime;
    }

    /**
     * 结束时间
     */
    public static String getStopRowkey(String caller, String startTime, String endTime, int partitions)
    {
        String hashcode = getHashcode(caller, startTime, partitions);
        return hashcode + "," + caller + "," + endTime;
    }


    /**
     * 计算查询时间范围
     */
    public static List<CallLogRange> getCallLogRanges(String startTime, String endTime)
    {
        try
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

            //存储时间段值
            List<CallLogRange> time_period_set = new ArrayList<CallLogRange>();

            if (startTime.compareTo(endTime) > 0)
            {
                System.out.println("起始时间错误，请正确输入");
                return null;
            }

            //日历对象
            Calendar cal = Calendar.getInstance();

            if (sdf.parse(startTime.substring(0, 6) + "01").equals(sdf.parse(endTime.substring(0, 6) + "01")))
            {
                CallLogRange range = new CallLogRange();
                range.setStartPoint(startTime);
                range.setEndPoint(endTime);
                time_period_set.add(range);
                return null;
            }


            while (true)
            {
                //设定开始时间月日 --- 结束时间月日
                CallLogRange range = new CallLogRange();
                range.setStartPoint(startTime);

                cal.setTime(sdf.parse(startTime.substring(0, 6) + "01"));
                cal.add(Calendar.MONTH, 1);

                //计算最后一个月时间范围
                if (cal.getTime().after(sdf.parse(endTime)))
                {
                    range.setEndPoint(endTime);
                    time_period_set.add(range);
//                    System.out.println("时间计算结束");
                    break;
                }

                range.setEndPoint(sdf.format(cal.getTime()));
                time_period_set.add(range);
                startTime = sdf.format(cal.getTime());
//                System.out.println(range);//调用toString方法
            }
            return time_period_set;
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 对时间进行格式化
     */
    public static String formatDate(String timeStr)
    {
        try
        {
            return sdfFriend.format(sdf.parse(timeStr));
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
}

