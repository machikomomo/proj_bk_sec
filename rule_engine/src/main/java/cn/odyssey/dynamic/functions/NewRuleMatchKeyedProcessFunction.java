package cn.odyssey.dynamic.functions;

import cn.odyssey.dynamic.beans.*;
import cn.odyssey.dynamic.controller.TriggerModelRuleMatchController;
import cn.odyssey.dynamic.service.TriggerModelRuleMatchServiceImpl;
import cn.odyssey.dynamic.utils.StateDescContainer;
import cn.odyssey.fact.flink_demo.CanalBean;
import cn.odyssey.fact.flink_demo.DBRecord;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.List;
import java.util.Map;

@Slf4j
public class NewRuleMatchKeyedProcessFunction extends KeyedBroadcastProcessFunction<String, LogBean, CanalBinlogBean, RuleMatchResult> {

    ListState<LogBean> listState;
    TriggerModelRuleMatchServiceImpl triggerModelRuleMatchService;

    @Override
    public void open(Configuration parameters) throws Exception {
        listState = getRuntimeContext().getListState(StateDescContainer.getLogBeansDesc());
        triggerModelRuleMatchService = new TriggerModelRuleMatchServiceImpl(listState);
    }

    @Override
    public void processElement(LogBean logBean, ReadOnlyContext readOnlyContext, Collector<RuleMatchResult> collector) throws Exception {
        listState.add(logBean);
        // 取状态
        ReadOnlyBroadcastState<String, RuleStateBean> broadcastState = readOnlyContext.getBroadcastState(StateDescContainer.MapStateDescriptor);
        Iterable<Map.Entry<String, RuleStateBean>> entries = broadcastState.immutableEntries();
        for (Map.Entry<String, RuleStateBean> entry : entries) {
            RuleStateBean ruleStateBean = entry.getValue();
            MarketingRule marketingRule = ruleStateBean.getMarketingRule();
            // kisSession
            KieSession kieSession = ruleStateBean.getKieSession();
            // fact
            RuleControllerFact ruleControllerFact = new RuleControllerFact(marketingRule, logBean, false, triggerModelRuleMatchService);
            kieSession.insert(ruleControllerFact);
            kieSession.fireAllRules();
            boolean matchResult = ruleControllerFact.isMatchResult();
            // if match
            if (matchResult) {
                RuleMatchResult ruleMatchResult = new RuleMatchResult(logBean.getDeviceId(), marketingRule.getRuleId(), logBean.getTimeStamp(), System.currentTimeMillis());
                log.debug("===============有一个完全匹配，logBean的deviceId为:{}，marketingRule_id为:{}===============", logBean.getDeviceId(), marketingRule.getRuleId());
                collector.collect(ruleMatchResult);
            }
        }
    }

    @Override
    public void processBroadcastElement(CanalBinlogBean canalBinlogBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        // 先去拿这个状态 往里面放东西
        BroadcastState<String, RuleStateBean> broadcastState = context.getBroadcastState(StateDescContainer.MapStateDescriptor);
        List<BinlogDataRecord> dbRecordList = canalBinlogBean.getData();
        String canalType = canalBinlogBean.getType();
        for (BinlogDataRecord binlogDataRecord : dbRecordList) {
            String rule_name = binlogDataRecord.getRule_name();
            String rule_condition_json = binlogDataRecord.getRule_condition_json();
            MarketingRule marketingRule = JSON.parseObject(rule_condition_json, MarketingRule.class);
            String rule_controller_drl = binlogDataRecord.getRule_controller_drl();
            String online = binlogDataRecord.getRule_status();

            // drl_string to kieSession 模版
            KieSession kieSession = new KieHelper().addContent(rule_controller_drl, ResourceType.DRL).build().newKieSession();

            RuleStateBean ruleStateBean = new RuleStateBean(marketingRule, kieSession);

            // ruleStateBean to state
            // 判断变更是什么操作 目的，后续处理广播流状态；判断依据，既要看canalType又要看online
            // 那insert时，也就是第一次放入规则时，online必须为1
            if ("INSERT".equals(canalType) || ("UPDATE".equals(canalType) && "1".equals(online))) {
                broadcastState.put(rule_name, ruleStateBean);
            } else if ("DELETE".equals(canalType) || ("UPDATE".equals(canalType) && "0".equals(online))) {
                broadcastState.remove(rule_name);
            }
        }
    }
}
