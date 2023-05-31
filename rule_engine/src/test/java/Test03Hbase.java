import cn.odyssey.engine.utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class Test03Hbase {
    public static void main(String[] args) throws IOException {
        // 连接hbase数据库，写入数据 10000*100col
        Config config = ConfigFactory.load();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        System.out.println("连接hbase完成！");

        // 写到哪个数据库，需要新建一张表 test_profile_tb
        // create 'test_profile_tb','f'
        Table table = hbaseConn.getTable(TableName.valueOf("test_profile_tb"));
        // 往里放一条数据
        // rowKey
        ArrayList<Put> data = new ArrayList<>();

        String rowNum = "007";
//        System.out.println(rowNum.getBytes()== Bytes.toBytes(rowNum));
        Put put = new Put(Bytes.toBytes(rowNum));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("col2"), Bytes.toBytes("valueX"));


        String rowNum2 = "006";
//        System.out.println(rowNum.getBytes()== Bytes.toBytes(rowNum));
        Put put2 = new Put(Bytes.toBytes(rowNum2));
        put2.addColumn(Bytes.toBytes("f"), Bytes.toBytes("col2"), Bytes.toBytes("valueX"));
        data.add(put);
        data.add(put2);
        table.put(data);


    }
}
