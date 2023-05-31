package cn.odyssey.engine.queryservice;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;

public class HbaseQueryServiceImpl implements QueryService {
    // 要有一个构造函数 传入hbaseConn
    Connection hbaseConn;

    public HbaseQueryServiceImpl(Connection hbaseConn) {
        this.hbaseConn = hbaseConn;
    }

    // 实现一个查询方法，返回true或false 传入deviceId和规则里的用户画像（需要里面的列）
    public boolean hbaseQuery(String deviceId, Map<String, String> ruleProfile) throws IOException {
        Table table = hbaseConn.getTable(TableName.valueOf("momo_profile_tb"));
        Get get = new Get(deviceId.getBytes()); // rowKey (查的时候是用getBytes)
        // 设置要查询的family和qualifier（标签名）
        for (String key : ruleProfile.keySet()) {
            get.addColumn("f".getBytes(), key.getBytes());
        }
        // 然后请求hbase查询 查到指定deviceId的人，指定列的用户画像值
        Result result = table.get(get);
        // 但是查出来是一整个，需要一个个拆出来比
        for (String key : ruleProfile.keySet()) {
            byte[] value = result.getValue("f".getBytes(), key.getBytes()); // 查出来的一个值
            String ruleValue = ruleProfile.get(key); // 规则里需要的值
            if (!ruleValue.equals(new String(value))) {
                return false;
            }
        }
        return true;
    }
}
