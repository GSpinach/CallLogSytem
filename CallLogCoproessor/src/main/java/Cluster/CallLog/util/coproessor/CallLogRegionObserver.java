package Cluster.CallLog.util.coproessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 协处理器coproessor
 */
public class CallLogRegionObserver extends BaseRegionObserver
{
    //被叫应用id
    private static final String REF_ROW_ID = "refrowid";
    //通话记录表名
    private static final String CALL_LOG_TABLE_NAME = "ns1:calllogs";

    /**
     * Put后处理
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException
    {
        super.postPut(e, put, edit, durability);
        //获取table名字
        String tableName0 = TableName.valueOf(CALL_LOG_TABLE_NAME).getNameAsString();
        //得到当前TableName对象
        String tableName1 = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //判断是否是ns1:calllogs表
        if (!tableName0.equals(tableName1))
        {
            return;
        }

        //得到主叫rowkey
        String rowkey = Bytes.toString(put.getRow());
        //如果被叫就放行
        String[] arr = rowkey.split(",");
        if (arr[3].equals("1"))
        {
            return;
        }

        String caller = arr[1]; //主叫
        String callTime = arr[2]; //通话时间
        String callee = arr[4];//被叫
        String callDuration = arr[5];//通话时长
        //获取hash值
        String hashcode = CallLogUtil.getHashcode(caller, callTime, 100);
        //被叫rowkey
        String calleRowkey = hashcode + "," + callee + "," + callTime + ",1," + caller + "," + callDuration;
        Put newPut = new Put(Bytes.toBytes(calleRowkey));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes(REF_ROW_ID), Bytes.toBytes(rowkey));
        TableName tn = TableName.valueOf(CALL_LOG_TABLE_NAME);
        Table t = e.getEnvironment().getTable(tn);
        t.put(newPut);
    }

    /**
     * 重写方法，完成被叫查询，返回主叫结果
     *
     * @param e
     * @param get
     * @param results
     * @throws IOException
     */
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException
    {
        //获取表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //判断是否是ns1:calllogs表
        if (!tableName.equals(CALL_LOG_TABLE_NAME))
        {
            super.postGetOp(e, get, results);
        } else
        {
            //得到主叫rowkey
            String rowkey = Bytes.toString(get.getRow());
            String[] arr = rowkey.split(",");
            //主叫
            if (arr[3].equals("0"))
            {
                super.postGetOp(e, get, results);
            } else //被叫
            {
                //得到主叫方的rowkey
                String refrowid = Bytes.toString(CellUtil.cloneValue(results.get(0)));
                Table tt = e.getEnvironment().getTable(TableName.valueOf(CALL_LOG_TABLE_NAME));
                Get g = new Get(Bytes.toBytes(refrowid));
                Result r = tt.get(g);
                List<Cell> newlist = r.listCells();
                results.clear();
                results.addAll(newlist);
            }
        }
    }

    /**
     * 重写postScannerNext方法，完成被叫查询，返回主叫结果
     *
     * @param e
     * @param s
     * @param results
     * @param limit
     * @param hasMore
     * @return
     * @throws IOException
     */
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException
    {
        boolean b = super.postScannerNext(e, s, results, limit, hasMore);

        //新集合
        List<Result> newList = new ArrayList<Result>();

        //获得表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        //判断表名是否是ns1:calllogs
        if (tableName.equals(CALL_LOG_TABLE_NAME))
        {
            Table tt = e.getEnvironment().getTable(TableName.valueOf(CALL_LOG_TABLE_NAME));
            for (Result r : results)
            {
                //rowkey
                String rowkey = Bytes.toString(r.getRow());
                String flag = rowkey.split(",")[3];
                //主叫
                if (flag.equals("0"))
                {
                    newList.add(r);
                }
                //被叫
                else
                {
                    //取出主叫号码
                    byte[] refrowkey = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes(REF_ROW_ID));
                    Get newGet = new Get(refrowkey);
                    newList.add(tt.get(newGet));
                }
            }
            results.clear();
            results.addAll(newList);
        }
        return b;
    }
}
