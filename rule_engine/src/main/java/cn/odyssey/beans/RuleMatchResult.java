package cn.odyssey.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RuleMatchResult {
    private String deviceId;
    private String ruleId;
    long trigEventTimeStamp;
    long matchTimeStamp;
}
