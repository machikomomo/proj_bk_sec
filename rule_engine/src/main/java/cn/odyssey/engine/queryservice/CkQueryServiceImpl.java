package cn.odyssey.engine.queryservice;

import cn.odyssey.engine.beans.EventParam;
import cn.odyssey.engine.beans.EventSequenceParam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CkQueryServiceImpl implements QueryService {
    // 先写构造器
    Connection ckConn;

    public CkQueryServiceImpl(Connection ckConn) {
        this.ckConn = ckConn;
    }

    // 查次数 传入deviceId，和要查的事件（外层逻辑已经取出来了，这里就只传入一个EventParam）
    public long getEventCount(String deviceId, EventParam eventParam, long queryStart, long queryEnd) throws SQLException {
        String querySql = eventParam.getQuerySql();
        PreparedStatement preparedStatement = ckConn.prepareStatement(querySql);
        preparedStatement.setString(1, deviceId);
        preparedStatement.setLong(2, queryStart);
        preparedStatement.setLong(3, queryEnd);
        ResultSet resultSet = preparedStatement.executeQuery();
        long result = 0;
        while (resultSet.next()) {
            result = resultSet.getLong(1);
        }
        return result;
    }

    // 查最多完成了几步 传入deviceId，和要查的行为序列条件（一个）
    public int queryEventSeqMaxStep(String deviceId, EventSequenceParam eventSequenceParam, long queryStart, long queryEnd) throws SQLException {
        String sequenceQuerySql = eventSequenceParam.getSequenceQuerySql();
        PreparedStatement preparedStatement = ckConn.prepareStatement(sequenceQuerySql);
        preparedStatement.setString(1, deviceId);
        preparedStatement.setLong(2, queryStart);
        preparedStatement.setLong(3, queryEnd);
        ResultSet resultSet = preparedStatement.getResultSet();
        int maxStep = 0;
        while (resultSet.next()) {
            for (int i = 1; i < eventSequenceParam.getEventSequence().size() + 1; i++) {
                int v = resultSet.getInt(i);
                if (v == 1) {
                    maxStep = eventSequenceParam.getEventSequence().size() - (i - 1);
                    break;
                }
            }
        }
        return maxStep;
    }

}
