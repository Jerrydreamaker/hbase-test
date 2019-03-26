package thread; /**
 * Created by Dreamaker on 2017/11/14.
 */
import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;
import util.HbaseUtil;

/**
 * 从hhase表中读取数据。
 */
public class HbaseReadThread extends Thread {

    private Connection connection;
    private static String tableName;
    //private int threadNum;
    private long fileNum;
    private int threadOrder;
    private long[] threadTime;
    public HbaseReadThread(Connection connection, String tableName, /*int threadNum,*/ long fileNum, int threadOrder, long[]
                      threadTime) {
        this.connection=connection;
        this.tableName=tableName;
        //this.threadNum=threadNum;
        this.fileNum=fileNum;
        this.threadOrder=threadOrder;
        this.threadTime=threadTime;
    }

    public void run() {
        System.out.println("Thread-" + this.threadOrder + ":  Start to work!");
        long threadStartTime=System.currentTimeMillis();
        for (int i=0;i<fileNum;i++){
            try {
                HbaseUtil.GetData(connection,tableName, Integer.toString(threadOrder*(int)fileNum+i), "test_columnFamily","1");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long threadEndTime=System.currentTimeMillis();
        threadTime[threadOrder]=threadEndTime-threadStartTime;
        System.out.println("Thread-"+threadOrder+":"+(threadEndTime-threadStartTime)+"ms");
    }
}
