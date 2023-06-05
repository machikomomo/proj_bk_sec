package cn.odyssey.dynamic.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * deviceId，后续对谁推送
 * ruleId，满足了哪个规则
 * 事件时间，匹配上的时间，便于后续监控
 */
@Data
@AllArgsConstructor
public class RuleMatchResult {
    private String deviceId;
    private String ruleId;
    long trigEventTimeStamp;
    long matchTimeStamp;
}