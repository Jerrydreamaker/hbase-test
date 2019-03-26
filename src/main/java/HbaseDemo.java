/**
 * Created by Dreamaker on 2017/11/14.
 */
import java.io.*;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import enum1.OPTYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import thread.HbaseReadThread;
import thread.HbaseWriteThread;
import util.SizeTransformUtil;
import util.HbaseUtil;

/**
 *测试主类。
 */
public class HbaseDemo {
    //打印在控制台上的用户使用说明。
    private static String USAGE= "java -jar hbase_test.jar -tablename tablename -optype write -fileNum 100 -fileSize 1KB -threadNum 10\n" +
            "java -jar hbase_test.jar -tablename tablename -optype read -fileNum 100 -fileSize 1KB -threadNum 10\n";
    private static OPTYPE optype=null;
    private static int fileNum;                //单线程写入文件个数
    private static long fileSize;               //写入文件大小
    private static int threadNum;               //读写文件线程数
    private static long[] threadTime;           //存储每个线程运行时间,引用前需初始化
    private static long execStartTime;          //任务开始执行时间
    private static long execEndTime;            //任务结束运行时间
    private static String tableName;            //写入文件的表名
    private static long execTime;               //记录任务执行时间
    private static Vector<Thread> threadVector;  //保存线程执行读写任务的线程
    private static CountDownLatch countDownLatch;;//使用CountDownLatch使主线程等待辅线程全部执行完毕。

    /**
     * 根据参数初始化变量。
      * @param args
     * @return
     */
    private static int init(String[] args){
        if(args.length==0){
            System.out.println(USAGE);
            System.out.println("Missing arguments!");
            return -1;
        }
        else {
            for (int i=0;i<args.length;i++){
                if (args[i].equals("-tablename")){
                    i++;
                    tableName=args[i];//读入存储文件的表名
                }
                else if (args[i].equals("-optype")) {
                    i++;
                    if (args[i] .equals("write")) {
                        optype = OPTYPE.WRITE;
                    } else if (args[i].equals("read")) {
                        optype = OPTYPE.READ;
                    } else {
                        System.err.println("Error argument optype!");
                    }
                }
                else if (args[i].equals("-fileNum")){
                    i++;
                    fileNum=Integer.valueOf(args[i]);
                }
                else if (args[i].equals("-threadNum")){
                    i++;
                    threadNum=Integer.valueOf(args[i]);
                }
                else if (args[i].equals("-fileSize")){
                    i++;
                    fileSize= SizeTransformUtil.parseSize(args[i]);
                }
            }
        }
        threadVector=new Vector<Thread>();//初始化threadVector
        countDownLatch=new CountDownLatch(threadNum);//初始化CountDownLatch
        return 0;

    }


    public static void main(String[] args) throws IOException, InterruptedException {

        int ret = init(args);//读取命令行输入参数，初始化变量。
        if (ret < 0) {
            System.exit(-1);
        }
        /*
        初始化Configuration
         */
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.addResource("hbase-site.xml");

        threadTime=new long[threadNum];//根据线程个数创建线程执行时间记录数组。
        Connection[] connections=new Connection[threadNum];//根据线程个数创建连接池。
        for (int i=0;i<threadNum;i++){
            connections[i]= HbaseUtil.getConnection();
        }

        switch (optype.ordinal()) {
            //如果操作类型是write。
            case (0):{
                Connection connection= HbaseUtil.getConnection();//获取连接
                HbaseUtil.CreateTable(connection,tableName, "test_columnFamily");//创建表
                connection.close();//关闭连接

                execStartTime=System.currentTimeMillis();//记录任务开始时间
                for (int i = 0; i < threadNum; i++) {
                    Thread childThread= new HbaseWriteThread(connections[i],tableName, /*threadNum,*/ fileNum, fileSize, i,threadTime);
                    //threadVector.add(childThread);
                    childThread.start();
                    countDownLatch.countDown();
                }
                countDownLatch.await();
            }
            case (1):{
                execStartTime=System.currentTimeMillis();//记录任务开始时间
                for (int i=0;i<threadNum;i++){
                    Thread childThread=new HbaseReadThread(connections[i],tableName,/*threadNum,*/fileNum,i,threadTime);
                    //threadVector.add(childThread);
                    childThread.start();
                    countDownLatch.countDown();
                }
                countDownLatch.await();
            }
            /*
            for (Thread thread:threadVector){
                thread.join();
            }*/
            /**
             * 打印执行结果。
             */
            System.out.println("####################################################");
            execEndTime=System.currentTimeMillis();
            execTime=execEndTime-execStartTime;//任务总运行时间
            System.out.println("Total Time:"+execTime);
            System.out.println("TotalFileNum:"+(fileNum*threadNum));//读写文件总数。
            System.out.println("OPS(N/s):"+(fileNum*threadNum*1000/execTime));//每秒读写文件数。
            System.out.println("ThreadNum"+threadNum);//线程数。
            long SumOfThreadTime=0;
            for (int i=0;i<threadNum;i++){
                SumOfThreadTime+=threadTime[i];
                System.out.println(threadTime[i]);
            }
            System.out.println("SumOfThreadTime:" + SumOfThreadTime);//线程总执行时间，小于任务总执行时间。差距越大，任务并发度越高。
            System.out.println("threadNum"+threadNum);
            System.out.println("ThreadAvgTime(ms):"+(SumOfThreadTime/threadNum));//线程平均执行时间，小于任务总执行时间。差距越大，任务并发度越高。
            return;
        }
    }
}