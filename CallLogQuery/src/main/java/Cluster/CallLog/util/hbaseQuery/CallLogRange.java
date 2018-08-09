package Cluster.CallLog.util.hbaseQuery;

public class CallLogRange
{
    private String startPoint;
    private String endPoint;

    public String getStartPoint()
    {
        return startPoint;
    }

    public void setStartPoint(String startPoint)
    {
        this.startPoint = startPoint;
    }

    public String getEndPoint()
    {
        return endPoint;
    }

    public void setEndPoint(String endPoint)
    {
        this.endPoint = endPoint;
    }

    @Override
    public String toString()
    {
        return "CallLogRange{" +
                "startPoint='" + startPoint + '\'' +
                ", endPoint='" + endPoint + '\'' +
                '}';
    }
}
