package cn.odyssey.back.functions;

import cn.odyssey.back.beans.LogBean;
import cn.odyssey.back.beans.MarketingRule;
import cn.odyssey.back.beans.RuleMatchResult;
import cn.odyssey.back.utils.RuleSimulatorNew;
import cn.odyssey.back.utils.StateDescContainer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class RuleMatchKeyedProcessFunctionV3 extends KeyedProcessFunction<String, LogBean, RuleMatchResult> {

    ListState<LogBean> logBeanState;
    ListState<Tuple2<MarketingRule, Long>> ruleTimerState;
    List<MarketingRule> marketingRuleList;

    @Override
    public void open(Configuration parameters) throws Exception {
        logBeanState = getRuntimeContext().getListState(StateDescContainer.getLogBeanDesc());
        ruleTimerState = getRuntimeContext().getListState(StateDescContainer.getRuleTimerDesc());
        MarketingRule rule0 = RuleSimulatorNew.getRule();
        marketingRuleList = Arrays.asList(rule0);
    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        logBeanState.add(logBean);
        for (MarketingRule marketingRule : marketingRuleList) {

        }

    }
}
