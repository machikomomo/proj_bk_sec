package cn.odyssey.marketing.utils;


import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.beans.LogBean;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
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

    public static int sequenceStrMatchRegexCount(String eventStr, String pattern) {
        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(eventStr);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        log.debug("正则表达式：{}, 匹配结果为{}, 字符串是：{}", pattern, count, eventStr);
        return count;
    }
}
