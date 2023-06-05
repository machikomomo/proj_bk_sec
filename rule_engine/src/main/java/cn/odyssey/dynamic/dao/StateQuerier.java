package cn.odyssey.dynamic.dao;

import cn.odyssey.dynamic.beans.EventCombinationCondition;
import cn.odyssey.dynamic.beans.EventCondition;
import cn.odyssey.dynamic.beans.LogBean;
import cn.odyssey.dynamic.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;
import java.util.List;

public class StateQuerier {

    ListState<LogBean> listState;

    public StateQuerier(ListState<LogBean> listState) {
        this.listState = listState;
    }

    /**
     * state，根据时间范围和组合行为，查符合要求的eventId（ck只用来取数）（ps，规则的eventId映射成独有数字1、2、3……）
     * 查询到的结果是一个字符串，类似"112212311112"
     */
    public String getEventCombinationConditionStr(EventCombinationCondition eventCombinationCondition,
                                                  long queryStart,
                                                  long queryEnd) throws Exception {
        // 从条件中取出该条件关心的事件们 这里和ck不一样，ck查的时候已经用sql过滤了，但是这里，需要去比较对象(id和属性）
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();

        StringBuilder sb = new StringBuilder();

        // 去state查
        Iterable<LogBean> logBeans = listState.get();
        for (LogBean logBean : logBeans) {
            if (logBean.getTimeStamp() >= queryStart && logBean.getTimeStamp() <= queryEnd) {
                for (int i = 0; i < eventConditionList.size(); i++) {
                    EventCondition eventCondition = eventConditionList.get(i);
                    if (EventUtil.eventMatchCondition(eventCondition, logBean)) {
                        sb.append(i + 1);
                        break; // 匹配上了，后面的条件事件就不继续走了
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * 上层方法调用这个。
     * 拿着查询到的结果，一个字符串，类似"112212311112"
     * 和规则中取出的正则表达式，匹配，返回匹配上的次数。在程序里算。
     * 传入一个eventCombinationCondition，和时间范围，返回在state中查询到的符合的事件组合出现的次数
     */
    public int queryEventCombinationConditionCount(EventCombinationCondition eventCombinationCondition,
                                                   long queryStart,
                                                   long queryEnd) throws Exception {
        String eventCombinationConditionStr = getEventCombinationConditionStr(eventCombinationCondition, queryStart, queryEnd);
        String matchPattern = eventCombinationCondition.getMatchPattern();
        return EventUtil.sequenceStrMatchRegexCount(eventCombinationConditionStr, matchPattern);
    }
}
