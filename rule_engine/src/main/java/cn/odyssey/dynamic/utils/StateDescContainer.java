package cn.odyssey.dynamic.utils;

import cn.odyssey.dynamic.beans.LogBean;
import cn.odyssey.dynamic.beans.MarketingRule;
import cn.odyssey.dynamic.beans.RuleStateBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 构造各种状态描述器 ListStateDescriptor
 */
public class StateDescContainer {
    public static ListStateDescriptor<LogBean> getLogBeansDesc() {
        ListStateDescriptor<LogBean> logBeansDesc = new ListStateDescriptor<>("logBeans", LogBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        logBeansDesc.enableTimeToLive(ttlConfig);
        return logBeansDesc;
    }

    public static ListStateDescriptor<Tuple2<MarketingRule, Long>> getRuleTimerStateDesc() {
        return new ListStateDescriptor<>("rule_timer", TypeInformation.of(new TypeHint<Tuple2<MarketingRule, Long>>() {
        }));
    }

    public static MapStateDescriptor<String, RuleStateBean> MapStateDescriptor = new MapStateDescriptor<String, RuleStateBean>("rule-broadcast", String.class, RuleStateBean.class);
    // rule name, marketing rule(json), kieSession (drl controller)
}
