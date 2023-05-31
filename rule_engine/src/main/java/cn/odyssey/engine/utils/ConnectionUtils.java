package cn.odyssey.engine.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 各类查询库的连接客户端 全都写静态方法就行 方便调用
 */
public class ConnectionUtils {
    static Config config = ConfigFactory.load();

    public static Connection getHbaseConn() throws IOException {
//        System.out.println("hbase连接中……");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
//        System.out.println("hbase连接完成！");
        return hbaseConn;
    }

    public static java.sql.Connection getCkConn() throws ClassNotFoundException, SQLException {
        String ckDriver = config.getString(ConfigNames.CK_JDBC_DRIVER);
        String ckUrl = config.getString(ConfigNames.CK_JDBC_URL);
        Class.forName(ckDriver);
        java.sql.Connection ckConn = DriverManager.getConnection(ckUrl);
        return ckConn;
    }

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        Connection hbaseConn = getHbaseConn();
        hbaseConn.getTable(TableName.valueOf("momo_profile_tb"));
        System.out.println("能取到表！");
        java.sql.Connection ckConn = getCkConn();
        System.out.println("能连接到ck！");
    }
}
