package cn.odyssey.engine.utils;

import cn.odyssey.beans.EventParam;
import cn.odyssey.beans.LogBean;

import java.util.Map;

public class EventCompare {
    // 传入，左边规则中的触发事件即EventParam，右边是logBean
    public static boolean compare(EventParam ruleEvent, LogBean logBean) {
        if (!ruleEvent.getEventId().equals(logBean.getEventId())) {
            return false;
        }
        Map<String, String> ruleMap = ruleEvent.getEventProperties();
        Map<String, String> logMap = logBean.getProperties();
        for (String key : ruleMap.keySet()) {
            if (!ruleMap.get(key).equals(logMap.get(key))) {
                return false;
            }
        }
        return true;
    }
}
