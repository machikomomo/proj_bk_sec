package cn.odyssey.marketing.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class ConnectionUtils {
    static Config config = ConfigFactory.load();

    public static Connection getHbaseConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }

    public static java.sql.Connection getCkConn() throws ClassNotFoundException, SQLException {
        String ckDriver = config.getString(ConfigNames.CK_JDBC_DRIVER);
        String ckUrl = config.getString(ConfigNames.CK_JDBC_URL);
        Class.forName(ckDriver);
        java.sql.Connection ckConn = DriverManager.getConnection(ckUrl);
        return ckConn;
    }

    public static Jedis getRedisConn() {
        String host = config.getString(ConfigNames.REDIS_HOST);
        int port = config.getInt(ConfigNames.REDIS_PORT);
        Jedis jedis = new Jedis(host, port); // 该方法不报异常，所以下方自己写一下，查看redis是否正常连接
        String ping = jedis.ping();
        if (StringUtils.isNotBlank(ping)) {
            log.debug("redis connection successfully created!");
        } else {
            log.debug("redis connection failed!");
        }
        return jedis;
    }
}
