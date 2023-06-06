package cn.odyssey.dynamic.beans;


import cn.odyssey.dynamic.beans.EventCombinationCondition;
import cn.odyssey.dynamic.beans.EventCondition;
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

    // 用户画像属性条件
    private Map<String, String> userProfileConditions;

    // 行为组合条件list
    private List<EventCombinationCondition> eventCombinationConditions;
//
//    // 规则匹配推送次数限制
//    private int matchLimit;

}
