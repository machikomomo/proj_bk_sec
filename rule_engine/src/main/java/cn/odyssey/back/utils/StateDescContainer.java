package cn.odyssey.back.utils;


import cn.odyssey.back.beans.LogBean;
import cn.odyssey.back.beans.MarketingRule;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class StateDescContainer {
    /**
     * 写static方法 返回状态描述器
     */
    public static ListStateDescriptor<LogBean> getLogBeanDesc() {
        ListStateDescriptor<LogBean> logBeanListStateDescriptor = new ListStateDescriptor<LogBean>("logBeans", LogBean.class);
        StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(2)).build();
        logBeanListStateDescriptor.enableTimeToLive(build);
        return logBeanListStateDescriptor;
    }

    public static ListStateDescriptor<Tuple2<MarketingRule, Long>> getRuleTimerDesc() {
        ListStateDescriptor<Tuple2<MarketingRule, Long>> ruleTimer = new ListStateDescriptor<>("ruleTimer", TypeInformation.of(new TypeHint<Tuple2<MarketingRule, Long>>() {
        }));
        return ruleTimer;
    }
}
