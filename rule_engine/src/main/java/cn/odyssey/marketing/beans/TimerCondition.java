package cn.odyssey.marketing.beans;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 规则定时条件封装对象
 */
@Data
@NoArgsConstructor
public class TimerCondition {

    private long timeLate;
    private List<EventCombinationCondition> eventCombinationConditionList;

}
