package Cluster.CallLog.util.hiveQuery;

import Cluster.CallLog.util.hbaseQuery.CallLog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CalllLogService
{
    //hiveServer2连接串
    private static String HIVE_URL = "jdbc:hive2://centos6:10000/mydb";
    private static String USERNAME = "ghb";
    private static String PASSWORD = "ghb123";
    //驱动程序类
    private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

    static
    {
        try
        {
            Class.forName(driverClass);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 查询最新的通话记录，使用hive进行mr查询
     */
    public CallLog findLatestCallLog(String phoneNum)
    {
        try
        {
            Connection conn = DriverManager.getConnection(HIVE_URL, USERNAME, PASSWORD);
            Statement st = conn.createStatement();
            String sql = "select * from ext_calllogs_in_hbase where id like '%" + phoneNum + "%' order by callTime desc limit 1";
            ResultSet rs = st.executeQuery(sql);
            CallLog log = null;
            if (rs.next())
            {
                log = new CallLog();
                log.setCaller(rs.getString("caller"));
                log.setCallee(rs.getString("caller"));
                log.setCallTime(rs.getString("callTime"));
                log.setCallDuration(rs.getString("callDuration"));
            }
            rs.close();
            return log;
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询指定人员指定年份中各个月份的通话次数
     */
    public List<CallLogStat> statCallLogsCount(String caller, String year)
    {
        List<CallLogStat> list = new ArrayList<CallLogStat>();
        try
        {
            Connection conn = DriverManager.getConnection(HIVE_URL, USERNAME, PASSWORD);
            Statement st = conn.createStatement();
            String sql = "select count(*) ,substr(calltime,1,6) from ext_calllogs_in_hbase " +
                    "where caller = '" + caller + "' and substr(calltime,1,4) == '" + year
                    + "' group by substr(calltime,1,6)";
            ResultSet rs = st.executeQuery(sql);
            while (rs.next())
            {
                CallLogStat logSt = new CallLogStat();
                logSt.setCount(rs.getInt(1));
                logSt.setYearMonth(rs.getString(2));
                list.add(logSt);
            }
            rs.close();
            return list;
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }


//    查询字符串子串
//-----------------
//
//    substring(str,0,6);        //mysql,子串的起始索引从1开始,后面是长度
//
//    substr(str,0,6);        //hive,
//
//    select count(*) ,calltime from ext_calllogs_in_hbase where caller ='xxx'
//    group by
//    calltime;
//
//    select count(*) ,substr(calltime,1,6)
//    from ext_calllogs_in_hbase
//    where caller = 'xxx'
//    group by
//
//    substr(calltime,1,6);
//
//    select count(*) ,substr(calltime,1,6)
//    from ext_calllogs_in_hbase
//    where caller = '15032293356'
//
//    and substr(calltime,1,4) =='2017'
//    group by
//
//    substr(calltime,1,6);

}
