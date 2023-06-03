package cn.odyssey.back.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 举例：3s内没有触发A组合行为
 */
@Data
@AllArgsConstructor
public class TimerCondition {
    private long timeLate; // 表示延时多久，即上述3s
    private List<EventCombinationCondition> eventCombinationConditionList;
}
