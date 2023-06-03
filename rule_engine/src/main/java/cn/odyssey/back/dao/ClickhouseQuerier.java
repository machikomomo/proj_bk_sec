package cn.odyssey.back.dao;

import cn.odyssey.back.beans.EventCombinationCondition;
import cn.odyssey.back.beans.EventCondition;
import cn.odyssey.back.utils.EventUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClickhouseQuerier {
    Connection ckConn;

    public ClickhouseQuerier(Connection ckConn) {
        this.ckConn = ckConn;
    }

    // 传入deviceId、一个行为组合、时间范围，输出查出来的eventId字符串
    public String getEventCombinationConditionStr(String deviceId,
                                                  EventCombinationCondition eventCombinationCondition,
                                                  long timeRangeStart,
                                                  long timeRangeEnd) throws SQLException {
        String querySql = eventCombinationCondition.getQuerySql();
        PreparedStatement preparedStatement = ckConn.prepareStatement(querySql);
        preparedStatement.setString(1, deviceId);
        preparedStatement.setLong(2, timeRangeStart);
        preparedStatement.setLong(3, timeRangeEnd);
        ResultSet resultSet = preparedStatement.executeQuery();

        // 将上面的查出来的结果转化成数字字符串，即每个eventId都有对应的数字
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        List<String> eventList = new ArrayList<>();
        for (EventCondition eventCondition : eventConditionList) {
            eventList.add(eventCondition.getEventId());
        }

        // 准备拼接
        StringBuilder sb = new StringBuilder();
        while (resultSet.next()) {
            String eventId = resultSet.getString(1);
            sb.append(eventList.indexOf(eventId) + 1);
        }
        return sb.toString();
    }

    // 上层调用这个方法
    public int queryEventCombinationConditionCount(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long timeRangeStart,
                                                   long timeRangeEnd) throws SQLException {
        String eventStr = getEventCombinationConditionStr(deviceId, eventCombinationCondition, timeRangeStart, timeRangeEnd);
        String matchPattern = eventCombinationCondition.getMatchPattern();
        return EventUtils.sequenceStrMatchRegexCount(eventStr, matchPattern);
    }
}
