package cn.odyssey.back.utils;

import cn.odyssey.back.beans.EventCondition;
import cn.odyssey.back.beans.LogBean;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventUtils {
    /**
     * 写一个static方法 左边是EventCondition，右边是LogBean，比较是否匹配
     */
    public static boolean eventMatchCondition(EventCondition eventCondition, LogBean logBean) {
        if (!eventCondition.getEventId().equals(logBean.getDeviceId())) {
            return false;
        }
        Set<String> keys = eventCondition.getEventProps().keySet();
        for (String key : keys) {
            if (!eventCondition.getEventProps().get(key).equals(logBean.getProperties().get(key))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 写一个static方法 左边是待比较的字符串 右边是pattern
     */
    public static int sequenceStrMatchRegexCount(String eventStr, String pattern) {
        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(eventStr);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
