package cn.odyssey.engine.queryservice;

import cn.odyssey.engine.beans.EventParam;
import cn.odyssey.engine.beans.LogBean;
import cn.odyssey.engine.utils.EventCompare;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * 去state查，一个序列匹配了多少步，返回步数
 */
public class StateQueryServiceImpl {

    ListState<LogBean> listState;

    public StateQueryServiceImpl(ListState<LogBean> listState) {
        this.listState = listState;
    }

    public int queryEventSequence(List<EventParam> eventSequence, long timeRangeStart, long timeRangeEnd) throws Exception {
        int i = 0;
        int count = 0;
        Iterable<LogBean> iterable = listState.get();
        for (LogBean logBean : iterable) {
            if (logBean.getTimeStamp() >= timeRangeStart && logBean.getTimeStamp() <= timeRangeEnd) {
                if (EventCompare.compare(eventSequence.get(i), logBean)) {
                    count++;
                    i++;
                    // 最后一个都对上了
                    if (i == eventSequence.size()) {
                        return count;
                    }
                }
            }
        }
        return count;
    }
}
