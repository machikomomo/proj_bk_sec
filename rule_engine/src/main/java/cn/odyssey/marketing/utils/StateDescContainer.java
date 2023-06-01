package cn.odyssey.marketing.utils;

import cn.odyssey.marketing.beans.LogBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * 构造各种状态描述器（我这就一个）
 */
public class StateDescContainer {
    public static ListStateDescriptor<LogBean> getLogBeansDesc() {
        ListStateDescriptor<LogBean> logBeansDesc = new ListStateDescriptor<>("logBeans", LogBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        logBeansDesc.enableTimeToLive(ttlConfig);
        return logBeansDesc;
    }
}
