package cn.odyssey.dynamic.beans;

import cn.odyssey.dynamic.service.TriggerModelRuleMatchServiceImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * KeiSession里insert fact
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleControllerFact {
    private MarketingRule marketingRule;
    private LogBean logBean;
    private boolean matchResult;
    private TriggerModelRuleMatchServiceImpl triggerModelRuleMatchService;
}
