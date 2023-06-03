package cn.odyssey.back.controller;

import cn.odyssey.back.beans.EventCombinationCondition;
import cn.odyssey.back.beans.EventCondition;
import cn.odyssey.back.beans.LogBean;
import cn.odyssey.back.beans.MarketingRule;
import cn.odyssey.back.utils.EventUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class TriggerModelRuleMatchController {
    // 传入 MarketingRule 和 LogBean
    public boolean ruleIsMatch(MarketingRule marketingRule, LogBean logBean) {
        // 判断触发事件是否匹配
        EventCondition triggerEventCondition = marketingRule.getTriggerEventCondition();
        if (!EventUtils.eventMatchCondition(triggerEventCondition, logBean)) {
            return false;
        }
        log.debug("触发事件匹配！");

        // 判断用户画像条件是否匹配
        Map<String, String> userProfileCondition = marketingRule.getUserProfileCondition();
        if (userProfileCondition!=null && userProfileCondition.size()>0){

        }

        // 判断行为组合list是否满足，一个个判断
        List<EventCombinationCondition> eventCombinationConditionList = marketingRule.getEventCombinationConditionList();


        return true;
    }
}
