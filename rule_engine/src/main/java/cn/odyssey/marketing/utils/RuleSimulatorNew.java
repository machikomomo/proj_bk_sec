package cn.odyssey.marketing.utils;


import cn.odyssey.marketing.beans.EventCombinationCondition;
import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.beans.MarketingRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RuleSimulatorNew {
    public static MarketingRule getRule() {
        MarketingRule ruleConditions = new MarketingRule();
        ruleConditions.setRuleId("rule_001");

        // 触发条件 注意：该条件只需要关心eventId和properties
        Map<String, String> map1 = new HashMap<>();
        map1.put("p2", "v1");
        EventCondition triggerEvent = new EventCondition("K", map1, -1, Long.MAX_VALUE, 1, 999);
        ruleConditions.setTriggerEventCondition(triggerEvent);

        // 用户画像条件 map
        Map<String, String> map2 = new HashMap<>();
//        map2.put("tag87", "v2");
        map2.put("tag26", "v1");
        ruleConditions.setUserProfileConditions(map2);

        // 单个行为条件列表 某个时间段某个事件发生
        String eventId = "C";
        Map<String, String> map3 = new HashMap<>();
//        map3.put("p3", "v1");
//        map3.put("p5", "v2");
        long startTime = -1L;
        long endTime = Long.MAX_VALUE;
        // 用于过滤的sql
        String sql1 = "SELECT\n" +
                "eventId\n" +
                "from momo_detail\n" +
                "where eventId = 'C'\n" +
                "and deviceId = ? and timeStamp BETWEEN ? and ? ";
        String rPattern1 = "(1)";
        EventCondition e = new EventCondition(eventId, map3, startTime, endTime, 1, 999);
        EventCombinationCondition eventGroupParam = new EventCombinationCondition(startTime, endTime, 1, 999, Arrays.asList(e), rPattern1, "ck", sql1, "001");

        // 多个行为条件组合 某个时间段某个事件发生

        long st = -1;
        long ed = Long.MAX_VALUE;
        String eventId1 = "A";
        Map<String, String> props1 = new HashMap<>();
//        props1.put("p8", "v1");
        EventCondition e1 = new EventCondition(eventId1, props1, st, ed, 1, 999);
        String eventId2 = "C";
        Map<String, String> props2 = new HashMap<>();
//        props2.put("p7", "v1");
        EventCondition e2 = new EventCondition(eventId2, props2, st, ed, 1, 999);
        String eventId3 = "F";
        Map<String, String> props3 = new HashMap<>();
//        props3.put("p6", "v1");
        EventCondition e3 = new EventCondition(eventId3, props3, st, ed, 1, 999);

        // 本来过滤的时候匹配事件是要带属性的，这里为了提高命中率就不带了
        String sql2 = "SELECT\n" +
                "eventId\n" +
                "from momo_detail\n" +
                "where deviceId = ?\n" +
                "and timeStamp BETWEEN ? and ? \n" +
                "and (\n" +
                "(eventId = 'A')\n" +
                "OR\n" +
                "(eventId = 'C')\n" +
                "OR\n" +
                "(eventId = 'F')\n" +
                ")";
        String rPattern2 = "(1.*2.*3)";
        EventCombinationCondition eventGroupParam2 = new EventCombinationCondition(st, ed, 1, 999, Arrays.asList(e1, e2, e3), rPattern2, "ck", sql2, "002");

        ruleConditions.setEventCombinationConditions(Arrays.asList(eventGroupParam, eventGroupParam2));

        return ruleConditions;
    }
}
