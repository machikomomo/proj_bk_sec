package cn.odyssey.beans;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RuleConditions {
    // 规则id
    private String ruleId;

    // 触发事件
    private EventParam triggerEvent;

    // 用户画像属性条件
    private Map<String, String> userProfileConditions;

    // 行为次数条件
    private List<EventParam> actionCountConditions;

    // 行为序列条件
    private List<EventSequenceParam> actionSequenceConditions;
}
