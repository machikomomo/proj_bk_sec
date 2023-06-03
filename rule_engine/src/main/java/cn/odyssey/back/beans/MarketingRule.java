package cn.odyssey.back.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingRule {
    // 规则id
    private String ruleId;
    // 触发事件
    private EventCondition triggerEventCondition;
    // 用户画像条件
    private Map<String, String> userProfileCondition;
    // 行为组合条件列表
    private List<EventCombinationCondition> eventCombinationConditionList;
    // 是否要注册timer
    private boolean onTimer;
    // 定时器时长条件
    private List<TimerCondition> timerConditionList;
}
