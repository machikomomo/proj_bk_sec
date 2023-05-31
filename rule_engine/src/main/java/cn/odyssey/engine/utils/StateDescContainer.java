package cn.odyssey.engine.utils;

import cn.odyssey.beans.LogBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * 构造各种 状态描述器
 */
public class StateDescContainer {

    /**
     * 近期行为事件存储状态描述器
     */
    public static ListStateDescriptor<LogBean> getLogBeansDesc() {
        ListStateDescriptor<LogBean> logBeansDesc = new ListStateDescriptor<>("logBeans", LogBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        logBeansDesc.enableTimeToLive(ttlConfig);
        return logBeansDesc;
    }
}
