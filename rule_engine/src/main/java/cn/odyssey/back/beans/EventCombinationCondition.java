package cn.odyssey.back.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 事件组合体封装
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCombinationCondition {
    private long timeRangeStart;
    private long timeRangeEnd;
    private int minLimit;
    private int maxLimit;

    // 关心的事件
    List<EventCondition> eventConditionList;
    // 正则表达式
    private String matchPattern;

    private String sqlType;
    private String querySql;

}
