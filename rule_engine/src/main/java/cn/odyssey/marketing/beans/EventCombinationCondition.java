package cn.odyssey.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 事件组合体封装 类似于[C !A B D]>=2 或者[A.*B.*C]
 * 用正则表达式去表示事件组合条件
 *
 * 步骤：
 * 0、eventConditionList即关心的事件列表，matchPattern表示正则表达式
 * 1、去clickhouse或者state中过滤出关心的事件（过滤条件包含整个事件即id和属性）
 * 2、过滤结果只需要存事件id即可（即一个eventId列表）
 * 3、拿到2的结果和正则表达式去匹配
 *
 * 所以下面的核心元素是eventConditionList和matchPattern
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCombinationCondition {
    private long timeRangeStart;
    private long timeRangeEnd;
    private int minLimit;
    private int maxLimit;

    private List<EventCondition> eventConditionList;
    private String matchPattern;

    // sql可以去clickhouse、dorisDB（用户事件明细库）等查询
    private String sqlType;
    // sql语句
    private String querySql;

    // 条件缓存id
    private String cacheId;
}
