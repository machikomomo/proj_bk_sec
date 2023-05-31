package cn.odyssey.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class EventParam {
    private String eventId;
    private Map<String, String> eventProperties;
    private int countThreshold;
    private long timeRangeStart;
    private long timeRangeEnd;
    private String querySql;
}
