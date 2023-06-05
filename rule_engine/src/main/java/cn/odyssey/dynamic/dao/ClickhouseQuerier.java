package cn.odyssey.dynamic.dao;


import cn.odyssey.dynamic.beans.EventCombinationCondition;
import cn.odyssey.dynamic.beans.EventCondition;
import cn.odyssey.dynamic.utils.EventUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class ClickhouseQuerier {
    Connection ckConn;

    public ClickhouseQuerier(Connection ckConn) {
        this.ckConn = ckConn;

    }

    /**
     * 在clickhouse中，根据时间范围和组合行为，查符合要求的eventId（ck只用来取数）（ps，规则的eventId映射成独有数字1、2、3……）
     * 查询到的结果是一个字符串，类似"112212311112"
     */
    public String getEventCombinationConditionStr(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryStart,
                                                   long queryEnd) throws SQLException {


        String querySql = eventCombinationCondition.getQuerySql();
        PreparedStatement preparedStatement = ckConn.prepareStatement(querySql);
        preparedStatement.setString(1, deviceId);
        preparedStatement.setLong(2, queryStart);
        preparedStatement.setLong(3, queryEnd);
        ResultSet resultSet = preparedStatement.executeQuery(); // 根据规则定义的sql，这里是select eventId from ... 所以查出来一堆eventId

        // 从条件中取出该条件关心的事件列表
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        List<String> conditionEvents = new ArrayList<>();
        for (EventCondition condition : eventConditionList) {
            conditionEvents.add(condition.getEventId());
        }
        StringBuilder sb = new StringBuilder();
        while (resultSet.next()) {
            String eventId = resultSet.getString(1);
            sb.append(conditionEvents.indexOf(eventId) + 1); // 拼接的数字从1开始
        }

        return sb.toString();
    }



    /**
     * 上层方法调用这个。
     * 拿着查询到的结果，一个字符串，类似"112212311112"
     * 和规则中取出的正则表达式，匹配，返回匹配上的次数。在程序里算。减少clickhouse的查询和计算压力。
     * 给deviceId，事件组合条件eventCombinationCondition，时间范围，返回查询到的符合的事件组合出现的次数
     */
    public int queryEventCombinationConditionCount(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryStart,
                                                   long queryEnd) throws SQLException {

        // 事件组合字符串
        String eventCombinationConditionStr = getEventCombinationConditionStr(deviceId, eventCombinationCondition, queryStart, queryEnd);
        // 取出正则表达式
        String matchPattern = eventCombinationCondition.getMatchPattern();
        log.debug("事件组合字符串:{}", eventCombinationConditionStr);

        return EventUtil.sequenceStrMatchRegexCount(eventCombinationConditionStr, matchPattern);
    }
}
