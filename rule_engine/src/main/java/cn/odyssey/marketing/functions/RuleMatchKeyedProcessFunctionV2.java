package cn.odyssey.marketing.functions;

import cn.odyssey.marketing.beans.*;
import cn.odyssey.marketing.controller.TriggerModelRuleMatchController;
import cn.odyssey.marketing.utils.RuleSimulatorNew;
import cn.odyssey.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class RuleMatchKeyedProcessFunctionV2 extends KeyedProcessFunction<String, LogBean, RuleMatchResult> {

    TriggerModelRuleMatchController triggerModelRuleMatchController;
    List<MarketingRule> ruleList;
    ListState<LogBean> listState;
    ListState<Tuple2<MarketingRule, Long>> ruleTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // flink里在open里构造
        MarketingRule rule1 = RuleSimulatorNew.getRule();
        ruleList = Arrays.asList(rule1);
        listState = getRuntimeContext().getListState(StateDescContainer.getLogBeansDesc());
        triggerModelRuleMatchController = new TriggerModelRuleMatchController(listState);
        // 记录规则定时（规则：定时）信息的state
        ruleTimerState = getRuntimeContext().getListState(StateDescContainer.getRuleTimerStateDesc());
    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<RuleMatchResult> collector) throws Exception {

        // 把数据流事件放到state
        listState.add(logBean);

        // 假设只要符合一个规则就行
        for (MarketingRule rule : ruleList) {
            boolean b = triggerModelRuleMatchController.ruleIsMatch(rule, logBean);
            if (b) {
                // 如果满足了触发条件（包括触发事件、用户画像、行为组合）
                if (rule.isOnTimer()) {
                    // 注册定时器
                    List<TimerCondition> timerConditionList = rule.getTimerConditionList();
                    TimerCondition timerCondition = timerConditionList.get(0); // 假设只有1个
                    long triggerTime = logBean.getTimeStamp() + timerCondition.getTimeLate();
                    context.timerService().registerEventTimeTimer(triggerTime);

                    //在定时信息state中进行记录
                    ruleTimerState.add(Tuple2.of(rule, triggerTime));

                } else {
                    RuleMatchResult ruleMatchResult = new RuleMatchResult(logBean.getDeviceId(), rule.getRuleId(), logBean.getTimeStamp(), System.currentTimeMillis());
                    collector.collect(ruleMatchResult);
                }
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RuleMatchResult> out) throws Exception {
        Iterable<Tuple2<MarketingRule, Long>> iterable = ruleTimerState.get();
        Iterator<Tuple2<MarketingRule, Long>> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            Tuple2<MarketingRule, Long> tp = iterator.next();
            // 判断这个规则:定时点，是否是本次触发点
            if (tp.f1 == timestamp) {
                MarketingRule rule = tp.f0;
                // 如果不对应 直接continue 不处理
                // 如果对应
                TimerCondition timerCondition = rule.getTimerConditionList().get(0);
                // 调用service
                // timestamp - timerCondition.getTimeLate() 就是注册点
                boolean matchTimeCondition = triggerModelRuleMatchController.isMatchTimeCondition(ctx.getCurrentKey(), timerCondition, timestamp - timerCondition.getTimeLate(), timestamp);
                // 清除已经检查完毕的规则定时点state信息
                iterator.remove();
                if (matchTimeCondition) {
                    RuleMatchResult ruleMatchResult = new RuleMatchResult(ctx.getCurrentKey(), rule.getRuleId(), timestamp, ctx.timerService().currentProcessingTime());
                    out.collect(ruleMatchResult);
                }
            }
            // 双保险 - 一般情况下不会出现
            if (tp.f1 < timestamp) {
                iterator.remove();
            }
        }
    }
}
