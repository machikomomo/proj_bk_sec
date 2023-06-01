package cn.odyssey.marketing.dao;


import cn.odyssey.marketing.beans.EventCombinationCondition;
import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.utils.ConnectionUtils;
import cn.odyssey.marketing.utils.EventUtil;
import cn.odyssey.marketing.utils.RuleSimulatorNew;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     */
    public int queryEventCombinationConditionCount(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryStart,
                                                   long queryEnd) throws SQLException {
        // 事件组合字符串
        String eventCombinationConditionStr = getEventCombinationConditionStr(deviceId, eventCombinationCondition, queryStart, queryEnd);
        // 取出正则表达式
        String matchPattern = eventCombinationCondition.getMatchPattern();
        log.debug("事件组合字符串:{}",eventCombinationConditionStr);

        return EventUtil.sequenceStrMatchRegexCount(eventCombinationConditionStr, matchPattern);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // 拼接字符串方法测试，没有问题
        Connection ckConn = ConnectionUtils.getCkConn();
        ClickhouseQuerier clickhouseQuerier = new ClickhouseQuerier(ckConn);
        String deviceId = "000002";
        List<EventCombinationCondition> eventCombinationConditions = RuleSimulatorNew.getRule().getEventCombinationConditions();
        EventCombinationCondition eventCombinationCondition = eventCombinationConditions.get(1);
        long st = -1;
        long ed = Long.MAX_VALUE;
        String eventCombinationConditionStr = clickhouseQuerier.getEventCombinationConditionStr(deviceId, eventCombinationCondition, st, ed);
        System.out.println(eventCombinationConditionStr);
        // FFF FFC AF
        // 333 332 13
        // 第二个方法，目前有一个问题就是 matchPattern 是数字吗？是的话就没问题，String rPattern2 = "(1.*2.*3)"; 比demo多了对括号，但是没问题
        int count = clickhouseQuerier.queryEventCombinationConditionCount(deviceId, eventCombinationCondition, st, ed);
        System.out.println(count);
        // 查询匹配次数方法，没有问题
    }
}
