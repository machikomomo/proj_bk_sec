package cn.odyssey.back.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

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
