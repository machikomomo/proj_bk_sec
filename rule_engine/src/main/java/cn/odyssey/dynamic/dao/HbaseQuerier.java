package cn.odyssey.dynamic.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;

public class HbaseQuerier {

    Table table;
    String familyName;

    // 外面传连接，外面传表名
    public HbaseQuerier(Connection hbaseConn, String profileTableName, String familyName) throws IOException {
        table = hbaseConn.getTable(TableName.valueOf(profileTableName));
        this.familyName = familyName;
    }

    /**
     * 从hbase中查询画像条件是否满足
     * @param deviceId 要查询的用户标识
     * @param ruleProfile 用户画像条件
     * @return 是否匹配
     * @throws IOException 异常
     */
    public boolean queryProfileIsMatch(String deviceId, Map<String, String> ruleProfile) throws IOException {
        Get get = new Get(deviceId.getBytes()); // rowKey (查的时候是用getBytes)
        // 设置要查询的family和qualifier（标签名）
        for (String key : ruleProfile.keySet()) {
            get.addColumn(familyName.getBytes(), key.getBytes());
        }
        // 然后请求hbase查询 查到指定deviceId的人，指定列的用户画像值
        Result result = table.get(get);
        // 但是查出来是一整个，需要一个个拆出来比
        for (String key : ruleProfile.keySet()) {
            byte[] value = result.getValue(familyName.getBytes(), key.getBytes()); // 查出来的一个值
            String v = new String(value); // 查出来的值new个String
            String ruleValue = ruleProfile.get(key); // 规则里需要的值
            if (StringUtils.isBlank(v) || !v.equals(ruleValue)) return false;
        }
        return true;
    }
}
