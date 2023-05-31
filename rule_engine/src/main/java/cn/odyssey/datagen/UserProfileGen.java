package cn.odyssey.datagen;

import cn.odyssey.engine.utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 用户画像数据有10w条，即10w个rowKey，每个用户有100个tag，每个tag的值为v1或v2
 *  000009 column=f:tag70, timestamp=1685427243661, value=v2
 */
public class UserProfileGen {
    public static void main(String[] args) throws IOException {
        // 连接hbase数据库，写入数据 10000*100col
        Config config = ConfigFactory.load();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);

        Table table = hbaseConn.getTable(TableName.valueOf("momo_profile_tb"));
        ArrayList<Put> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            String account = StringUtils.leftPad(i + "", 6, "0");
            Put put = new Put(Bytes.toBytes(account));
            for (int k = 1; k <= 100; k++) {
                String key = "tag" + k;
                String value = "v" + RandomUtils.nextInt(1, 3);
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
            }
            data.add(put);
            if (data.size() == 100) {
                table.put(data);
                data.clear();
            }
        }
        if (data.size() > 0) {
            table.put(data);
        }
        hbaseConn.close();

    }
}
