package cn.odyssey.dynamic.beans;

import cn.odyssey.dynamic.beans.EventCondition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 事件组合体封装 类似于[C !A B D]>=2 或者[A.*B.*C]
 * 用正则表达式去表示事件组合条件
 * <p>
 * 步骤：
 * 0、eventConditionList即关心的事件列表，matchPattern表示正则表达式
 * 1、去clickhouse或者state中过滤出关心的事件（过滤条件包含整个事件即id和属性）
 * 2、过滤结果只需要存事件id即可（即一个eventId列表）
 * 3、拿到2的结果和正则表达式去匹配
 * <p>
 * 所以下面的核心元素是eventConditionList和matchPattern
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCombinationCondition {
    // 构成组合以后，发生的范围和发生的次数
    private long timeRangeStart;
    private long timeRangeEnd;
    private int minLimit;
    private int maxLimit;

    private List<EventCondition> eventConditionList; // 待组合的事件条件

    private String matchPattern; // 用一个正则表达式组合

    // 对于关心的事件，sql可以去clickhouse、dorisDB（用户事件明细库）等查询
    private String sqlType;

    // sql语句
    private String querySql;

    // 条件缓存id 每个事件条件都带一个缓存id 也叫条件id
    private String cacheId;
}
