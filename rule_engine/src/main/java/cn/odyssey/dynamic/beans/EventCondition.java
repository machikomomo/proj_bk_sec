package cn.odyssey.dynamic.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 规则条件中，最原子的封装，封装一个事件条件
 * 某个时间段，某个事件（id和属性），发生的次数（最大次数、最小次数）
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCondition {
    private String eventId;
    private Map<String, String> eventProps;
//    private long timeRangeStart;
//    private long timeRangeEnd;
//    private int minLimit;
//    private int maxLimit;
}
