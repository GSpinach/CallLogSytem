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
        callers.put("15810092493", "史玉龙");
        callers.put("18000696806", "赵贺彪");
        callers.put("15151889601", "张倩");
        callers.put("13269361119", "王世昌");
        callers.put("15032293356", "张涛");
        callers.put("17731088562", "张阳");
        callers.put("15338595369", "李进全");
        callers.put("15733218050", "杜泽文");
        callers.put("15614201525", "任宗阳");
        callers.put("15778423030", "梁鹏");
        callers.put("18641241020", "郭美彤");
        callers.put("15732648446", "刘飞飞");
        callers.put("13341109505", "段光星");
        callers.put("13560190665", "唐会华");
        callers.put("18301589432", "杨力谋");
        callers.put("13520404983", "温海英");
        callers.put("18332562075", "朱尚宽");
        callers.put("18620192711", "刘能宗");
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
