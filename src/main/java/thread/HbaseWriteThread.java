package thread; /**
 * Created by Dreamaker on 2017/11/14.
 */
import org.apache.hadoop.hbase.client.Connection;
import util.HbaseUtil;

import java.io.IOException;

/**
 * 将数据写入hhase表中。
 */
public class HbaseWriteThread extends Thread {
    private static Connection connection;//Hbase连接
    //private static int threadNum;        //线程总数
    private static String tableName;    //表名
    private static long fileNum;         //单线程写入文件个数
    private static long fileSize;         //写入文件大小
    private int threadOrder;             //线程号
    private static byte[] writeBuffer;
    private static long[] threadTime;

    public HbaseWriteThread(Connection connection, String tableName/*, int threadNum*/, long fileNum, long fileSize,
                            int threadOrder, long[] threadTime) throws IOException {
        this.connection=connection;
        this.tableName=tableName;
        //this.threadNum=threadNum;
        this.fileNum=fileNum;
        this.fileSize = fileSize;
        this.threadOrder=threadOrder;
        this.writeBuffer = new byte[(int)fileSize];//强转，不建议使用
        this.threadOrder = threadOrder;
        this.threadTime=threadTime;
        /*
        初始化写入内容
         */
        for (int i = 0; i < this.fileSize; i++)
            this.writeBuffer[i] = (byte) ('0' + i % 50);
    }

    public void run() {
        long threadStartTime = System.currentTimeMillis();
        System.out.println("Thread-" + this.threadOrder + ":  Start to work!");
        for (int i=0;i<fileNum;i++) {
            try {
                HbaseUtil.PutData(connection, tableName, Integer.toString(threadOrder* (int)fileNum + i), "test_columnFamily", "1",writeBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long threadEndTime=System.currentTimeMillis();
        threadTime[threadOrder] = threadEndTime - threadStartTime;
        System.out.println("Thread-"+threadOrder+":"+(threadEndTime-threadStartTime)+"ms");
    }
}

