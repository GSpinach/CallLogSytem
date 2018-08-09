package Cluster.CallLog.util.callloggen;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class APP
{
    public static List<String> phoneNumbers = new ArrayList<String>();
    public static Map<String, String> callers = new HashMap<String, String>();

    static
    {
        callers.put("15812234243", "张三");
        callers.put("17657696806", "李四");
        callers.put("13434455601", "高一");
        callers.put("13234561119", "波斯");
        callers.put("15324227756", "高二");
        callers.put("17737887562", "高三");
        callers.put("15313459869", "高四");
        callers.put("13453218050", "大一");
        callers.put("19123445625", "大二");
        callers.put("13454678860", "大三");
        callers.put("11235251020", "大四");
        callers.put("15657648446", "研一");
        callers.put("13567678505", "研二");
        callers.put("13337652365", "研三");
        callers.put("15678897432", "博一");
        callers.put("18798904983", "波尔");
        callers.put("18468389795", "播散");
        callers.put("10975645554", "王五");
        phoneNumbers.addAll(callers.keySet());
    }

    public static void main(String[] args) throws IOException, InterruptedException
    {
        if (args.length == 0 | args == null)
        {
            System.out.println("No args, Please enter the output path to parameter.");
            System.exit(-1);
        }
        genCallLog(args[0]);
    }

    public static void genCallLog(String arg) throws IOException, InterruptedException
    {
        //随机生成器
        Random random = new Random();

        //输出数据，初始化
        FileWriter fw = new FileWriter(arg);

        while (true)
        {
            //取主叫号码、姓名
            String caller = phoneNumbers.get(random.nextInt(callers.size()));
            String callerName = callers.get(caller);

            //取被叫号码、姓名
            String callee = null;
            String calleeName = null;
            while (true)
            {
                callee = phoneNumbers.get(random.nextInt(callers.size()));
                if (!caller.equals(callee))
                {
                    break;
                }
            }
            calleeName = callers.get(callee);

            //通话时长
            int duringTime = random.nextInt(60 * 10) + 1;

            //通话时间
            int year = 2018;
            //月份(0~11)
            int month = random.nextInt(12);
            //天,范围(1~31)
            int day = random.nextInt(29) + 1;
            int hour = random.nextInt(24);
            int min = random.nextInt(60);
            int sec = random.nextInt(60);

            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month);
            cal.set(Calendar.DAY_OF_MONTH, day);
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, min);
            cal.set(Calendar.SECOND, sec);
            Date phoneDate = cal.getTime();

            //通话时间（如果时间超过今天就重新获取）
            Date nowDate = new Date();
            if (phoneDate.after(nowDate))
            {
                continue;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            String dateStr = sdf.format(phoneDate);
            String log = caller + "," + callee + "," + dateStr + "," + duringTime;
            System.out.println(log);
            fw.write(log + "\r\n");
            fw.flush();
            Thread.sleep(200);
        }
    }
}
