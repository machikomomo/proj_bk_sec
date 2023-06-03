package cn.odyssey.back.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HbaseQuerier {

    Table table;
    String familyName;

    public HbaseQuerier(Connection hbaseConn, String tableName, String familyName) throws IOException {
        table = hbaseConn.getTable(TableName.valueOf(tableName));
        this.familyName = familyName;
    }

    // 传入deviceId和用户画像条件map，返回true or false
    public boolean queryProfileIsMatch(String deviceId, Map<String, String> userProfile) throws IOException {
        Get get = new Get(deviceId.getBytes());
        Set<String> keys = userProfile.keySet();
        for (String key : keys) {
            get.addColumn(familyName.getBytes(), key.getBytes());
        }
        Result result = table.get(get);
        for (String key : keys) {
            byte[] value = result.getValue(familyName.getBytes(), key.getBytes());
            String s = new String(value); // 从hbase里查出来的值
            String ruleValue = userProfile.get(key); // 规则里需要的值
            if (StringUtils.isBlank(s) || !ruleValue.equals(s)) {
                return false;
            }
        }
        return true;
    }
}
