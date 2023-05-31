package cn.odyssey.router;

import cn.odyssey.beans.EventParam;
import cn.odyssey.beans.EventSequenceParam;
import cn.odyssey.beans.LogBean;
import cn.odyssey.beans.RuleConditions;
import cn.odyssey.engine.queryservice.CkQueryServiceImpl;
import cn.odyssey.engine.queryservice.HbaseQueryServiceImpl;
import cn.odyssey.engine.utils.ConnectionUtils;
import cn.odyssey.engine.utils.EventCompare;
import cn.odyssey.engine.utils.SegmentQueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Map;

/**
 * 远期近期分段查询路由器
 */
@Slf4j
public class NearFarSegmentQueryRouter {
    Connection hbaseConn;
    java.sql.Connection ckConn;
    HbaseQueryServiceImpl hbaseQueryService;
    CkQueryServiceImpl ckQueryService;

    public NearFarSegmentQueryRouter() throws Exception {
        hbaseConn = ConnectionUtils.getHbaseConn();
        hbaseQueryService = new HbaseQueryServiceImpl(hbaseConn);
        ckConn = ConnectionUtils.getCkConn();
        ckQueryService = new CkQueryServiceImpl(ckConn);
    }

    public boolean ruleMatch(RuleConditions rule, LogBean logBean, ListState<LogBean> beansState) throws Exception {
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

        // TODO 待修改 先拿到当前事件的时间和分界点 clickhouse 查startTime到分界点
        long segmentPoint = SegmentQueryUtil.getSegmentPoint(logBean.getTimeStamp());

        // TODO 该deviceId去查clickhouse，返回的long值（count一般是long值），是否能够达到规则里的次数阈值
        List<EventParam> actionCountConditions = rule.getActionCountConditions();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventParam actionCountCondition : actionCountConditions) {
                int ruleCnt = actionCountCondition.getCountThreshold();
                // 判断 跨时区、全在左、全在右
                if (actionCountCondition.getTimeRangeEnd() < segmentPoint) {

                    // 全在左
                    long queryCnt = ckQueryService.getEventCount(logBean.getDeviceId(), actionCountCondition, actionCountCondition.getTimeRangeStart(), actionCountCondition.getTimeRangeEnd());
                    if (queryCnt < ruleCnt) return false;
                } else if (actionCountCondition.getTimeRangeStart() >= segmentPoint) {

                    // 全在右
                    long stateQueryCount = stateQueryActionCount(beansState, actionCountCondition, actionCountCondition.getTimeRangeStart(), actionCountCondition.getTimeRangeEnd());
                    if (stateQueryCount < ruleCnt) return false;
                } else {

                    // 跨界 注意：先查状态，如果状态满足了就不再查clickhouse了。注意查状态时起始时间变成segmentPoint
                    long stateQueryCount = stateQueryActionCount(beansState, actionCountCondition, segmentPoint, actionCountCondition.getTimeRangeEnd());
                    if (stateQueryCount < ruleCnt) {
                        // 继续去ck查 查的时候sql传入start为分界点
                        long ckCount = ckQueryService.getEventCount(logBean.getDeviceId(), actionCountCondition, actionCountCondition.getTimeRangeStart(), segmentPoint);
                        if ((stateQueryCount + ckCount) < ruleCnt) return false;
                    }
                }
                log.debug("当前deviceId指定的用户行为次数条件，要求执行事件{}，要求执行{}次", actionCountCondition, ruleCnt);
            }
        }
        log.debug("当前deviceId满足了指定的用户行为次数条件，计算匹配上的时间为{}", System.currentTimeMillis());

        // TODO 该deviceId去查clickhouse，返回boolean，行为序列条件是否满足
        List<EventSequenceParam> actionSequenceConditions = rule.getActionSequenceConditions();
        if (actionSequenceConditions != null & actionSequenceConditions.size() > 0) {
            // 对每个序列条件都去查
            for (EventSequenceParam actionSequenceCondition : actionSequenceConditions) {

                if (actionSequenceCondition.getTimeRangeEnd() < segmentPoint) {
                    // 全在左 只查ck
                    int maxStep = ckQueryService.queryEventSeqMaxStep(logBean.getDeviceId(), actionSequenceCondition); // 查到的最多完成了几步
                    int nowSeqSize = actionSequenceCondition.getEventSequence().size();  // 当前序列条件包含的event，即需要完成的事件总数
                    if (maxStep < nowSeqSize) {
                        return false;
                    }
                } else if (actionSequenceCondition.getTimeRangeStart() >= segmentPoint) {
                    // 全在右 只查状态


                } else {
                    // 跨时区 先查状态 再查ck

                }

                log.debug("当前deviceId满足了指定的行为序列条件，要求执行序列{}", actionSequenceCondition);
            }
        }
        log.debug("当前deviceId满足了指定的行为序列条件，计算匹配上的时间为{}", System.currentTimeMillis());
        return true;
    }


    /**
     * 工具方法：在flink state中计算指定事件的发生次数
     * 后续可以自定义查询的起始时间（目前是采用规则定义的起始和结束时间）
     *
     * @param beansState
     * @param actionCountCondition
     * @param queryStart
     * @param queryEnd
     * @return
     * @throws Exception
     */
    private long stateQueryActionCount(ListState<LogBean> beansState, EventParam actionCountCondition, long queryStart, long queryEnd) throws Exception {
        long count = 0;
        Iterable<LogBean> iter = beansState.get();
        for (LogBean bean : iter) {
            // 判断遍历到的事件是否落在规则条件的时间区间内
            if (bean.getTimeStamp() >= queryStart && bean.getTimeStamp() <= queryEnd) {
                // 判断这个logBean的事件条件与actionCountCondition的事件条件是否相等，相等则计数器+1
                if (EventCompare.compare(actionCountCondition, bean)) {
                    count += 1;
                }
            }
        }
        return count;
    }
}
