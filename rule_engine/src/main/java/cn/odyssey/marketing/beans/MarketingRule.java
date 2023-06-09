package cn.odyssey.marketing.beans;


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

    // 规则匹配推送次数限制
    private int matchLimit;

    // 是否要注册timer
    private boolean onTimer;

    // 定时器时长条件 对于一个logBean，可以设置多个定时器，所以这里是个list，实际上测试的时候就一个
    private List<TimerCondition> timerConditionList;

}
