package cn.odyssey.back.utils;

import cn.odyssey.back.beans.EventCombinationCondition;
import cn.odyssey.back.beans.EventCondition;
import cn.odyssey.back.beans.MarketingRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RuleSimulatorNew {
    /**
     * 写一个static方法，一调用就get一个rule
     */
    public static MarketingRule getRule() {
        MarketingRule marketingRule = new MarketingRule();

        marketingRule.setRuleId("rule_001");

        // 触发条件 注意：该条件只需要关心eventId和properties
        Map<String, String> map1 = new HashMap<>();
        map1.put("p2", "v1");
        EventCondition triggerEvent = new EventCondition("K", map1, -1, Long.MAX_VALUE, 1, 999);
        marketingRule.setTriggerEventCondition(triggerEvent);

        // 用户画像条件
        Map<String, String> map2 = new HashMap<>();
        map2.put("tag26", "v1");
        marketingRule.setUserProfileCondition(map2);

        // 行为组合条件
        // 第一个行为组合
        String eventId = "C";
        Map<String, String> map3 = new HashMap<>();
        long start1 = -1L;
        long end1 = Long.MAX_VALUE;
        String sql1 = "SELECT\n" +
                "eventId\n" +
                "from momo_detail\n" +
                "where eventId = 'C'\n" +
                "and deviceId = ? and timeStamp BETWEEN ? and ? ";
        String r = "(1)";
        EventCondition eventCondition = new EventCondition(eventId, map3, start1, end1, 1, 999);
        EventCombinationCondition e1 = new EventCombinationCondition(start1, end1, 1, 999, Arrays.asList(eventCondition), r, "ck", sql1);

        // 第二个行为组合
        long start2 = -1L;
        long end2 = Long.MAX_VALUE;
        String eventId1 = "A";
        Map<String, String> props1 = new HashMap<>();
        EventCondition eventCondition1 = new EventCondition(eventId1, props1, start2, end2, 1, 999);
        String eventId2 = "C";
        Map<String, String> props2 = new HashMap<>();
        EventCondition eventCondition2 = new EventCondition(eventId2, props2, start2, end2, 1, 999);
        String eventId3 = "F";
        Map<String, String> props3 = new HashMap<>();
        EventCondition eventCondition3 = new EventCondition(eventId3, props3, start2, end2, 1, 999);
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
        String r2 = "(1.*2.*3)";
        EventCombinationCondition e2 = new EventCombinationCondition(start2, end2, 1, 999, Arrays.asList(eventCondition1, eventCondition2, eventCondition3), r2, "ck", sql2);
        marketingRule.setEventCombinationConditionList(Arrays.asList(e1, e2));
        return marketingRule;
    }
}
