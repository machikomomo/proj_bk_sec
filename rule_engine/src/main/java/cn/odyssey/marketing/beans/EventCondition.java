package cn.odyssey.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 规则条件中，最原子的封装，封装一个事件条件
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCondition {
    private String eventId;
    private Map<String, String> eventProps;
    private long timeRangeStart;
    private long timeRangeEnd;
    private int minLimit;
    private int maxLimit;
}
