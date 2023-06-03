package cn.odyssey.back.utils;

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

/**
 * 返回各类数据库连接 调用static方法
 */
@Slf4j
public class ConnectionUtils {

    static Config config = ConfigFactory.load();

    public static Connection getHbaseConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static java.sql.Connection getCkConn() throws ClassNotFoundException, SQLException {
        String ckDriver = config.getString(ConfigNames.CK_JDBC_DRIVER);
        String ckUrl = config.getString(ConfigNames.CK_JDBC_URL);
        Class.forName(ckDriver);
        java.sql.Connection connection = DriverManager.getConnection(ckUrl);
        return connection;
    }

    public static Jedis getRedisConn() {
        String host = config.getString(ConfigNames.REDIS_HOST);
        int port = config.getInt(ConfigNames.REDIS_PORT);
        Jedis jedis = new Jedis(host, port);
        // 因为redis创建连接不会报错，所以下面手动ping一下，确认是否成功连接
        String ping = jedis.ping();
        if (StringUtils.isNotBlank(ping)) {
            log.debug("Redis is successfully connected!");
        } else {
            log.debug("Redis connected failed!");
        }
        return jedis;
    }
}
