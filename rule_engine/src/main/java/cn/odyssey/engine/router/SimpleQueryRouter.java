package cn.odyssey.engine.router;

import cn.odyssey.engine.beans.EventParam;
import cn.odyssey.engine.beans.EventSequenceParam;
import cn.odyssey.engine.beans.LogBean;
import cn.odyssey.engine.beans.RuleConditions;
import cn.odyssey.engine.queryservice.CkQueryServiceImpl;
import cn.odyssey.engine.queryservice.HbaseQueryServiceImpl;
import cn.odyssey.engine.utils.ConnectionUtils;
import cn.odyssey.engine.utils.EventCompare;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleQueryRouter {
    Connection hbaseConn;
    java.sql.Connection ckConn;
    HbaseQueryServiceImpl hbaseQueryService;
    CkQueryServiceImpl ckQueryService;

    public SimpleQueryRouter() throws Exception {
        hbaseConn = ConnectionUtils.getHbaseConn();
        hbaseQueryService = new HbaseQueryServiceImpl(hbaseConn);
        ckConn = ConnectionUtils.getCkConn();
        ckQueryService = new CkQueryServiceImpl(ckConn);
    }

    public boolean ruleMatch(RuleConditions rule, LogBean logBean) throws Exception {
        // TODO 该logBean中的事件是否是规则里的触发事件 写一个类，定义静态方法
        if (!EventCompare.compare(rule.getTriggerEvent(), logBean)) {
            return false;
        }
        log.debug("当前事件满足了触发事件{}，计算匹配上的时间为{}", logBean.getEventId(), System.currentTimeMillis());

        // TODO 该deviceId去查用户画像是否和规则里的用户画像吻合 先取出规则里的画像条件 如果为null就跳过了
        Map<String, String> userProfileConditions = rule.getUserProfileConditions();
        if (userProfileConditions != null) {
            boolean res = hbaseQueryService.hbaseQuery(logBean.getDeviceId(), rule.getUserProfileConditions());
            if (!res) {
                return false;
            }
        }
        log.debug("当前deviceId满足了指定的用户画像{}，计算匹配上的时间为{}", rule.getUserProfileConditions(), System.currentTimeMillis());

        // TODO 该deviceId去查clickhouse，返回的long值（count一般是long值），是否能够达到规则里的次数阈值
        List<EventParam> actionCountConditions = rule.getActionCountConditions();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventParam actionCountCondition : actionCountConditions) {
                // 每个条件依次查（实际我只放了一个条件）
                long queryCnt = ckQueryService.getEventCount(logBean.getDeviceId(), actionCountCondition, actionCountCondition.getTimeRangeStart(), actionCountCondition.getTimeRangeEnd());
                int ruleCnt = actionCountCondition.getCountThreshold();
                if (queryCnt < ruleCnt) {
                    return false;
                }
                log.debug("当前deviceId满足了指定的用户行为次数条件，要求执行事件{}，要求执行{}次，实际查到{}次", actionCountCondition, ruleCnt, queryCnt);
            }
        }
        log.debug("当前deviceId满足了指定的用户行为次数条件，计算匹配上的时间为{}", System.currentTimeMillis());

        // TODO 该deviceId去查clickhouse，返回boolean，行为序列条件是否满足
        List<EventSequenceParam> actionSequenceConditions = rule.getActionSequenceConditions();
        if (actionSequenceConditions != null & actionSequenceConditions.size() > 0) {
            // 对每个序列条件都去查
            for (EventSequenceParam actionSequenceCondition : actionSequenceConditions) {
                int maxStep = ckQueryService.queryEventSeqMaxStep(logBean.getDeviceId(), actionSequenceCondition, actionSequenceCondition.getTimeRangeStart(), actionSequenceCondition.getTimeRangeEnd()); // 查到的最多完成了几步
                int nowSeqSize = actionSequenceCondition.getEventSequence().size();  // 当前序列条件包含的event，即需要完成的事件总数
                if (maxStep < nowSeqSize) {
                    return false;
                }
                log.debug("当前deviceId满足了指定的行为序列条件，要求执行序列{}，一共有{}个序列，实际最多完成{}个", actionSequenceCondition, nowSeqSize, maxStep);
            }
        }
        log.debug("当前deviceId满足了指定的行为序列条件，计算匹配上的时间为{}", System.currentTimeMillis());
        return true;
    }
}
