package cn.odyssey.marketing.functions;

import cn.odyssey.marketing.beans.EventCondition;
import cn.odyssey.marketing.beans.LogBean;
import cn.odyssey.marketing.beans.MarketingRule;
import cn.odyssey.marketing.beans.RuleMatchResult;
import cn.odyssey.marketing.utils.EventUtil;
import cn.odyssey.marketing.utils.RuleSimulatorNew;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, LogBean, RuleMatchResult> {

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        // TODO 获取规则
        MarketingRule rule = RuleSimulatorNew.getRule();

        // TODO 是否符合触发事件
        EventCondition triggerEvent = rule.getTriggerEvent();
        if (!EventUtil.eventMatchCondition(triggerEvent, logBean)) {
            return;
        }

        // TODO 是否符合用户画像条件
        Map<String, String> userProfileConditions = rule.getUserProfileConditions();
//        if (userProfileConditions != null) {
//            boolean res = hbaseQueryService.hbaseQuery(logBean.getDeviceId(), rule.getUserProfileConditions());
//            if (!res) {
//                return false;
//            }
//        }
        // TODO 是否符合行为组合条件

    }
}
