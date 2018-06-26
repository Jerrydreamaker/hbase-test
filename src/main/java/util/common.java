package util; /**
 * Created by Dreamaker on 2017/11/14.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.FileImageOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class common {
    private static final long MEGA;

    public common() {
    }

    public static float toMB(long bytes) {
        return (float)bytes / (float)MEGA;
    }

    public static long parseSize(String arg) {
        String[] args = arg.split("\\D", 2);
        assert args.length <= 2;
        //System.out.println("hello"+args[0]);
        long nrBytes = Integer.parseInt(args[0]);
        String bytesMult = arg.substring(args[0].length());
        return nrBytes * ByteMultiple.parseString(bytesMult).value();
    }

    public static Map<String, String> analysizeData(long[] rawData) {
        HashMap resultMap = new HashMap();
        Arrays.sort(rawData);
        int index_50 = (int)((double)rawData.length * 0.5D);
        int index_60 = (int)((double)rawData.length * 0.6D);
        int index_70 = (int)((double)rawData.length * 0.7D);
        int index_80 = (int)((double)rawData.length * 0.8D);
        int index_90 = (int)((double)rawData.length * 0.9D);
        int index_95 = (int)((double)rawData.length * 0.95D);
        resultMap.put("Min", String.valueOf(rawData[0]));
        resultMap.put("Max", String.valueOf(rawData[rawData.length - 1]));
        resultMap.put("50", String.valueOf(rawData[index_50]));
        resultMap.put("60", String.valueOf(rawData[index_60]));
        resultMap.put("70", String.valueOf(rawData[index_70]));
        resultMap.put("80", String.valueOf(rawData[index_80]));
        resultMap.put("90", String.valueOf(rawData[index_90]));
        resultMap.put("95", String.valueOf(rawData[index_95]));
        return resultMap;
    }

    static {
        MEGA = ByteMultiple.MB.value();
    }

    static enum ByteMultiple {
        B(1L),
        KB(1024L),
        MB(1048576L),
        GB(1073741824L),
        TB(1099511627776L);

        private long multiplier;

        private ByteMultiple(long mult) {
            this.multiplier = mult;
        }

        long value() {
            return this.multiplier;
        }

        static ByteMultiple parseString(String sMultiple) {
            if(sMultiple != null && !sMultiple.isEmpty()) {
                String sMU = sMultiple.toUpperCase();
                if(B.name().toUpperCase().endsWith(sMU)) {
                    return B;
                } else if(KB.name().toUpperCase().endsWith(sMU)) {
                    return KB;
                } else if(MB.name().toUpperCase().endsWith(sMU)) {
                    return MB;
                } else if(GB.name().toUpperCase().endsWith(sMU)) {
                    return GB;
                } else if(TB.name().toUpperCase().endsWith(sMU)) {
                    return TB;
                } else {
                    throw new IllegalArgumentException("Unsupported ByteMultiple " + sMultiple);
                }
            } else {
                return MB;
            }
        }
    }
    public static Connection getConnection() {
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        //hbaseConfiguration.addResource("hbase-site.xml");
        hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");

        hbaseConfiguration.set("hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3");
        hbaseConfiguration.addResource("hbase-site.xml");
        hbaseConfiguration.addResource("hbase-default.xml");
        Connection connection = null;
        try {
            connection=ConnectionFactory.createConnection(hbaseConfiguration);
            //connection=ConnectionFactory.createConnection()
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
    /**
     * 创建表
     * @param tablename 表名
     * @param columnFamily 列族
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static void CreateTable(Connection connection,String tablename,String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        System.out.println("未开始连接："+new Date(System.currentTimeMillis()));
        Admin admin = connection.getAdmin();
        System.out.println("连接成功："+new Date(System.currentTimeMillis()));
        TableName tableName =TableName.valueOf(tablename);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        System.out.println("HTableDescriptor创建成功" + new Date(System.currentTimeMillis()));
        tableDesc.addFamily(new HColumnDescriptor(columnFamily));
        System.out.println("addFamily成功" + new Date(System.currentTimeMillis()));
        admin.createTable(tableDesc);
        System.out.println(tablename+"表已经成功创建!");
    }

    /**
     * 将图片转为byte[]
     * @param path 图片输入路径
     * @return
     * @throws IOException
     */
    public static byte[] image2byte(String path) throws IOException {
        byte[]data=null;
        FileImageInputStream input=new FileImageInputStream(new File(path));
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        byte[]buf=new byte[1024];
        int num=0;
        while ((num=input.read(buf))!=-1){
            output.write(buf,0,num);
        }
        data=output.toByteArray();
        output.close();
        input.close();
        return data;
    }

    /**
     * 将byte[]转为图片
     * @param data  byte[]
     * @param path  图片输出路径
     * @throws IOException
     */
    public static void byte2image(byte[] data,String path) throws IOException {
        if (data.length<3||path.equals("")) return;
        FileImageOutputStream output=new FileImageOutputStream(new File(path));
        output.write(data,0,data.length);
        output.close();
        System.out.println("图片输出到" + path + "成功");

    }

    /**
     * 向表中插入一条新数据
     * @param tableName 表名
     * @param row 行键key
     * @param columnFamily 列族
     * @param column 列名
     * @param data 要插入的数据
     * @throws IOException
     */
    public static void PutData(Connection connection,String tableName,String row,String columnFamily,String column,byte[] data) throws IOException{

        TableName tableName1 = TableName.valueOf(tableName);
        Table table = connection.getTable(tableName1);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), data);
        table.put(put);
        System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
    }

    /**
     * 获取指定行的所有数据
     * @param tableName 表名
     * @param row 行键key
     * @param columnFamily 列族
     * @param column 列名
     * @param path 图片输出路径
     * @throws IOException
     */
    public static void GetData(Connection connection,String tableName,String row,String columnFamily,String column) throws IOException{
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = connection.getTable(tableName1);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        //byte2image(rb,path);
        //String value = new String(rb,"UTF-8");
        //System.out.println(value);
    }
}

