package cn.odyssey.marketing.functions;

import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.beans.LogBean;
import cn.odyssey.marketing.beans.MarketingRule;
import cn.odyssey.marketing.beans.RuleMatchResult;
import cn.odyssey.marketing.controller.TriggerModelRuleMatchController;
import cn.odyssey.marketing.utils.EventUtil;
import cn.odyssey.marketing.utils.RuleSimulatorNew;
import cn.odyssey.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, LogBean, RuleMatchResult> {

    TriggerModelRuleMatchController triggerModelRuleMatchController;
    List<MarketingRule> ruleList;
    ListState<LogBean> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // flink里在open里构造
        MarketingRule rule1 = RuleSimulatorNew.getRule();
        ruleList = Arrays.asList(rule1);
        listState = getRuntimeContext().getListState(StateDescContainer.getLogBeansDesc());
        triggerModelRuleMatchController = new TriggerModelRuleMatchController(listState);
    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<RuleMatchResult> collector) throws Exception {

        // 把数据流事件放到state
        listState.add(logBean);
//        log.debug("接收到数据流:{} ", logBean);

        // 假设只要符合一个规则就行
        for (MarketingRule rule : ruleList) {
//            log.debug("遍历到一个规则，ID:{} ", rule.getRuleId());
            boolean b = triggerModelRuleMatchController.ruleIsMatch(rule, logBean);
//            log.debug("规则计算完毕，规则id是{}，结果为{}", rule.getRuleId(), b);
            if (b) {
                RuleMatchResult ruleMatchResult = new RuleMatchResult(logBean.getDeviceId(), rule.getRuleId(), logBean.getTimeStamp(), System.currentTimeMillis());
                log.debug("===============有一个完全匹配，logBean的deviceId为:{}", logBean.getDeviceId());
                // logBean的deviceId为:009239
                // logBean的deviceId为:009841
                // logBean的deviceId为:007198
                // logBean的deviceId为:007494
                // logBean的deviceId为:006870
                // ......
                collector.collect(ruleMatchResult);
            }
        }
    }
}
