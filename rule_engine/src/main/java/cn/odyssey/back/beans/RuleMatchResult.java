package cn.odyssey.back.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RuleMatchResult {
    private String deviceId;
    private String ruleId;
    private long triggerEventTimeStamp;
    private long matchTimeStamp;
}
