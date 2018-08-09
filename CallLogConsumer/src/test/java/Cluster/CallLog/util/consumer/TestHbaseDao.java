package Cluster.CallLog.util.consumer;

import org.junit.Test;

public class TestHbaseDao
{
    @Test
    public void test()
    {
        HbaseDao dao = new HbaseDao();
        dao.put("15032293356,18641241020,2018/04/03 05:34:11,450");
    }
}
