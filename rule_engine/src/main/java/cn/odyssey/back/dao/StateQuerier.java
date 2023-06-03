package cn.odyssey.back.dao;

import cn.odyssey.back.beans.EventCombinationCondition;
import cn.odyssey.back.beans.EventCondition;
import cn.odyssey.back.beans.LogBean;
import cn.odyssey.back.utils.EventUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

public class StateQuerier {
    ListState<LogBean> listState;

    public StateQuerier(ListState<LogBean> listState) {
        this.listState = listState;
    }

    public String getEventCombinationConditionStr(EventCombinationCondition eventCombinationCondition,
                                                  long queryStart,
                                                  long queryEnd) throws Exception {
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        StringBuilder sb = new StringBuilder();
        Iterable<LogBean> logBeans = listState.get();
        for (LogBean logBean : logBeans) {
            if (logBean.getTimeStamp() >= queryStart && logBean.getTimeStamp() <= queryEnd) {
                for (int i = 0; i < eventConditionList.size(); i++) {
                    EventCondition eventCondition = eventConditionList.get(i);
                    if (EventUtils.eventMatchCondition(eventCondition, logBean)) {
                        sb.append(i + 1);
                        break;
                    }
                }
            }
        }
        return sb.toString();
    }

    public int queryEventCombinationConditionCount(EventCombinationCondition eventCombinationCondition,
                                                   long queryStart,
                                                   long queryEnd) throws Exception {
        String eventCombinationConditionStr = getEventCombinationConditionStr(eventCombinationCondition, queryStart, queryEnd);
        String matchPattern = eventCombinationCondition.getMatchPattern();
        return EventUtils.sequenceStrMatchRegexCount(eventCombinationConditionStr, matchPattern);
    }
}
