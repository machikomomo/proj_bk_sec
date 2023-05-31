package cn.odyssey.engine.utils;

import cn.odyssey.beans.EventParam;
import cn.odyssey.beans.EventSequenceParam;
import cn.odyssey.beans.RuleConditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleSimulator {
    public static RuleConditions getRule() {
        RuleConditions ruleConditions = new RuleConditions();
        ruleConditions.setRuleId("rule_001");

        // 触发条件 只需要关心eventId和properties
        Map<String, String> map1 = new HashMap<>();
        map1.put("p2", "v1");
        EventParam eventParam = new EventParam("K", map1, 0, -1, -1, null);
        ruleConditions.setTriggerEvent(eventParam);

        // 用户画像条件 map
        Map<String, String> map2 = new HashMap<>();
        map2.put("tag87", "v2");
        map2.put("tag26", "v1");
        ruleConditions.setUserProfileConditions(map2);

        // 行为次数条件 xx事件（eventId和properties）在startTime和endTime之间，发生过n次（countThreshold）
        // "C" "p3", "v1" "p5", "v2" 发生过1次
        Map<String, String> map3 = new HashMap<>();
        map3.put("p3", "v1");
        map3.put("p5", "v2");
        long startTime = 1623945600000L; // 2021-06-18 00:00:00
        long endTime = Long.MAX_VALUE; // 从startTime至今
        // 到clickhouse中查询指定deviceId的一个人，在startTime和endTime之间，是否发生过n次xx事件，查完了继续查下一个条件
        String sql = "" +
                "SELECT\n" +
                "count(1)\n" +
                "FROM momo_detail\n" +
                "where eventId = 'C' and properties['p3']='v1' and properties['p5']='v2' and deviceId = ? and timeStamp BETWEEN " + startTime + " and " + endTime;
        // 这里我们比较简单，只有一组条件，就是C事件，但还是用list存放
        EventParam eventParam1 = new EventParam("C", map3, 1, startTime, endTime, sql);
        ruleConditions.setActionCountConditions(Arrays.asList(eventParam1));

        /**
         * 行为次数条件确定以后，只要接受到一个event，取出其中的deviceId，就可以去clickhouse查，这个人
         * 在特定时间段内，完成'C'事件（具体属性也吻合）的次数====>可以查到一个int值
         * SELECT
         * COUNT(1)
         * FROM momo_detail
         * where eventId = 'C'
         * and properties['p3']='v1' and properties['p5']='v2'
         * and deviceId = '002065'
         * and timeStamp BETWEEN 1623945600000 and 9623945600000
         * ;
         */

        // TODO 行为次序条件
        // 一个行为次序条件，是一个List<EventParam>，但是可能有多个条件，目前只考虑一个条件
        // 造一个List<EventParam>，假设有3个EventParam，只关心从x时间到y时间，z事件有就行（包含属性）时间要求假设和上面一样
        String eventId1 = "O";
        Map<String, String> props1 = new HashMap<>();
        props1.put("p8", "v1");
        EventParam e1 = new EventParam(eventId1, props1, -1, startTime, endTime, null);
        String eventId2 = "F";
        Map<String, String> props2 = new HashMap<>();
        props2.put("p7", "v1");
        EventParam e2 = new EventParam(eventId2, props2, -1, startTime, endTime, null);
        String eventId3 = "R";
        Map<String, String> props3 = new HashMap<>();
        props3.put("p6", "v1");
        EventParam e3 = new EventParam(eventId3, props3, -1, startTime, endTime, null);
        List<EventParam> eventSeq = Arrays.asList(e1, e2, e3);
        // clickhouse查 三个都匹配记为match3，两个匹配记为match2，一个匹配记为match1
        String seq1sql = "SELECT\n" +
                "  sequenceMatch('.*(?1).*(?2).*(?3).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'O',\n" +
                "    eventId = 'F',\n" +
                "    eventId = 'R' \n" +
                "  ) as isMatch3,\n" +
                "\n" +
                "  sequenceMatch('.*(?1).*(?2).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'O',\n" +
                "    eventId = 'F',\n" +
                "    eventId = 'R' \n" +
                "  ) as isMatch2,\n" +
                "\n" +
                "  sequenceMatch('.*(?1).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'O',\n" +
                "    eventId = 'F',\n" +
                "    eventId = 'R' \n" +
                "  ) as isMatch1\n" +
                "\n" +
                "from momo_detail\n" +
                "where\n" +
                "  deviceId = ?\n" +
                "    and\n" +
                "  timeStamp between ? and ?\n" +
                "    and\n" +
                "  (\n" +
                "        (eventId = 'O' and properties['p8']='v1')\n" +
                "     or (eventId = 'F' and properties['p7']='v1')\n" +
                "     or (eventId = 'R' and properties['p6']='v1')\n" +
                "  )\n" +
                "group by deviceId;";
        EventSequenceParam eventSequenceParam = new EventSequenceParam("rule_001", startTime, endTime, eventSeq, seq1sql);
        ruleConditions.setActionSequenceConditions(Arrays.asList(eventSequenceParam));

        //用sql去查会查到这样的结果（从clickhouse中查到）
//        ┌─isMatch3─┬─isMatch2─┬─isMatch1─┐
//        │        1 │        1 │        1 │
//        └──────────┴──────────┴──────────┘


        return ruleConditions;
    }

    public static void main(String[] args) {
        RuleConditions rule = getRule();
        System.out.println(rule.getActionSequenceConditions());
    }
}
