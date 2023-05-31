package cn.odyssey.engine.utils;

import cn.odyssey.beans.LogBean;
import org.apache.flink.api.common.state.ListStateDescriptor;

/**
 * 构造各种 状态描述器
 */
public class StateDescContainer {

    /**
     * 近期行为事件存储状态描述器
     */
    public static ListStateDescriptor<LogBean> logBeansDesc = new ListStateDescriptor<>("logBeans", LogBean.class);
}
