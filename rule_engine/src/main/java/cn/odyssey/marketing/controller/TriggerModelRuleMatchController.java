package cn.odyssey.marketing.controller;

import cn.odyssey.marketing.beans.*;
import cn.odyssey.marketing.service.TriggerModelRuleMatchServiceImpl;
import cn.odyssey.marketing.utils.EventUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 触发模型的controller
 */
@Slf4j
public class TriggerModelRuleMatchController {

    TriggerModelRuleMatchServiceImpl triggerModelRuleMatchService;

    /**
     * 构造函数
     *
     * @param listState flink的状态句柄
     * @throws Exception 异常
     */
    public TriggerModelRuleMatchController(ListState<LogBean> listState) throws Exception {
        triggerModelRuleMatchService = new TriggerModelRuleMatchServiceImpl(listState);
    }

    /**
     * @param marketingRule 营销规则封装对象
     * @param logBean       数据流的事件
     * @return 规则是否匹配
     */
    public boolean ruleIsMatch(MarketingRule marketingRule, LogBean logBean) throws Exception {

        // 触发条件是否匹配
        EventCondition triggerEventCondition = marketingRule.getTriggerEventCondition();
        if (!EventUtil.eventMatchCondition(triggerEventCondition, logBean)) {
            return false;
        }
        log.debug("触发条件匹配！！！");

        // 用户画像条件是否匹配
        Map<String, String> userProfileConditions = marketingRule.getUserProfileConditions();
        if (userProfileConditions != null && userProfileConditions.size() > 0) {
            boolean profileRes = triggerModelRuleMatchService.matchProfileCondition(logBean.getDeviceId(), userProfileConditions);
            if (!profileRes) {
                return false;
            }
        }
        log.debug("用户画像条件匹配！！！");


        // 行为组合条件是否匹配
        List<EventCombinationCondition> eventCombinationConditions = marketingRule.getEventCombinationConditions();
        if (eventCombinationConditions != null && eventCombinationConditions.size() > 0) {
            for (EventCombinationCondition eventCombinationCondition : eventCombinationConditions) {
                boolean b = triggerModelRuleMatchService.matchEventCombinationCondition(eventCombinationCondition, logBean);
                if (!b) {
                    return false; // 此时是且关系，也就是要求每个行为组合都匹配，才是完全匹配 后续这部分放到动态规则系统实现
                }
                log.debug("行为组合条件中至少有一个组合匹配！！！");
            }
        }


        return true;

    }

    /**
     * 只检查定时条件是否满足
     *
     * @param deviceId
     * @param timerCondition
     * @param queryTimeStart
     * @param queryTimeEnd
     * @return
     */
    public boolean isMatchTimeCondition(String deviceId, TimerCondition timerCondition, long queryTimeStart, long queryTimeEnd) throws Exception {
        List<EventCombinationCondition> eventCombinationConditionList = timerCondition.getEventCombinationConditionList();
        for (EventCombinationCondition eventCombinationCondition : eventCombinationConditionList) {
            eventCombinationCondition.setTimeRangeStart(queryTimeStart);
            eventCombinationCondition.setTimeRangeEnd(queryTimeEnd);
            LogBean logBean = new LogBean();
            logBean.setDeviceId(deviceId);
            logBean.setTimeStamp(queryTimeEnd);// 时间戳设置为闹钟响起的那个当前时间
            boolean b = triggerModelRuleMatchService.matchEventCombinationCondition(eventCombinationCondition, logBean);
            if (!b) {
                return false;
            }
        }
        return true;
    }
}
