package cn.odyssey.engine.functions;

import cn.odyssey.engine.beans.*;
import cn.odyssey.engine.utils.RuleSimulator;
import cn.odyssey.engine.utils.StateDescContainer;
import cn.odyssey.engine.router.NearFarSegmentQueryRouter;
import cn.odyssey.engine.router.SimpleQueryRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, LogBean, RuleMatchResult> {

    SimpleQueryRouter simpleQueryRouter;
    ListState<LogBean> beansState;

    @Override
    public void open(Configuration parameters) throws Exception {
        simpleQueryRouter = new SimpleQueryRouter();
        beansState = getRuntimeContext().getListState(StateDescContainer.getLogBeansDesc());
        NearFarSegmentQueryRouter nearFarSegmentQueryRouter = new NearFarSegmentQueryRouter(beansState);
    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<RuleMatchResult> collector) throws Exception {

        // TODO 将当前收到的logBean存入flink state（状态）
        beansState.add(logBean);

        // TODO 拿到规则
        RuleConditions rule = RuleSimulator.getRule();

        // TODO 进行简单匹配
        boolean matchResult = simpleQueryRouter.ruleMatch(rule, logBean);
        if (!matchResult) {
            return;
        }

        // TODO 全部计算完成
        RuleMatchResult ruleMatchResult = new RuleMatchResult(logBean.getDeviceId(), rule.getRuleId(), logBean.getTimeStamp(), System.currentTimeMillis());
        collector.collect(ruleMatchResult);
    }
}
