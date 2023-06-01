package cn.odyssey.marketing.utils;


import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.beans.LogBean;

import java.util.Map;

public class EventUtil {
    // 传入，左边规则中的触发事件即EventCondition，右边是logBean
    public static boolean eventMatchCondition(EventCondition ruleEvent, LogBean logBean) {
        if (!ruleEvent.getEventId().equals(logBean.getEventId())) {
            return false;
        }
        Map<String, String> ruleMap = ruleEvent.getEventProps();
        Map<String, String> logMap = logBean.getProperties();
        for (String key : ruleMap.keySet()) {
            if (!ruleMap.get(key).equals(logMap.get(key))) {
                return false;
            }
        }
        return true;
    }
}
