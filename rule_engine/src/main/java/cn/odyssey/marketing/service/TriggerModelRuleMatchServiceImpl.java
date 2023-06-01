package cn.odyssey.marketing.service;

import cn.odyssey.marketing.beans.EventCombinationCondition;
import cn.odyssey.marketing.beans.LogBean;
import cn.odyssey.marketing.dao.ClickhouseQuerier;
import cn.odyssey.marketing.dao.HbaseQuerier;
import cn.odyssey.marketing.dao.StateQuerier;
import cn.odyssey.marketing.utils.ConfigNames;
import cn.odyssey.marketing.utils.ConnectionUtils;
import cn.odyssey.marketing.utils.CrossTimeQueryUtil;
import cn.odyssey.marketing.utils.EventUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class TriggerModelRuleMatchServiceImpl {

    // 成员变量
    ClickhouseQuerier clickhouseQuerier;
    HbaseQuerier hbaseQuerier;
    StateQuerier stateQuerier;

    // 构造函数
    // 构造DAO(3个）
    public TriggerModelRuleMatchServiceImpl(ListState<LogBean> listState) throws Exception {
        Config config = ConfigFactory.load();

        java.sql.Connection ckConn = ConnectionUtils.getCkConn();
        clickhouseQuerier = new ClickhouseQuerier(ckConn);

        Connection hbaseConn = ConnectionUtils.getHbaseConn();
        hbaseQuerier = new HbaseQuerier(hbaseConn, config.getString(ConfigNames.HBASE_PROFILE_TABLE), config.getString(ConfigNames.HBASE_PROFILE_FAMILY));

        stateQuerier = new StateQuerier(listState);
    }

    /**
     * 用户画像条件是否匹配
     */
    public boolean matchProfileCondition(String deviceId, Map<String, String> profileCondition) throws IOException {
        return hbaseQuerier.queryProfileIsMatch(deviceId, profileCondition);
    }

    /**
     * 计算1个组合条件是否匹配
     */
    public boolean matchEventCombinationCondition(EventCombinationCondition eventCombinationCondition, LogBean logBean) throws Exception {
        // 获取当前事件对应的查询分界点
        long segmentPoint = CrossTimeQueryUtil.getSegmentPoint(logBean.getTimeStamp());
        long conditionStart = eventCombinationCondition.getTimeRangeStart();
        long conditionEnd = eventCombinationCondition.getTimeRangeEnd();
        int minLimit = eventCombinationCondition.getMinLimit();
        int maxLimit = eventCombinationCondition.getMaxLimit();
        // 判断规则的时间段是否跨分界点
        if (conditionStart >= segmentPoint) {

            // 查state
            log.debug("来state查了！");
            int count = stateQuerier.queryEventCombinationConditionCount(eventCombinationCondition, conditionStart, conditionEnd);
            return count >= minLimit && count <= maxLimit;
        } else if (conditionEnd < segmentPoint) {

            // 查ck
            log.debug("来ck查了！");
            int count = clickhouseQuerier.queryEventCombinationConditionCount(logBean.getDeviceId(), eventCombinationCondition, conditionStart, conditionEnd);
            return count >= minLimit && count <= maxLimit;
        } else {

            // 两边查cross 先查state再查ck，改一下时间
            log.debug("两边查cross！");
            int count = stateQuerier.queryEventCombinationConditionCount(eventCombinationCondition, segmentPoint, conditionEnd);
            if (count >= minLimit && count <= maxLimit) {
                return true;
            }
            // AFFFF FFCCCCF
            // 拼接字符串，再去正则匹配
            String ckStr = clickhouseQuerier.getEventCombinationConditionStr(logBean.getDeviceId(), eventCombinationCondition, conditionStart, segmentPoint);
            String stateStr = stateQuerier.getEventCombinationConditionStr(eventCombinationCondition, segmentPoint, conditionEnd);
            int countBoth = EventUtil.sequenceStrMatchRegexCount(ckStr + stateStr, eventCombinationCondition.getMatchPattern());
            return countBoth >= minLimit && count <= maxLimit;
        }
    }
}
