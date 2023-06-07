package cn.odyssey.dynamic.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kie.api.runtime.KieSession;

/**
 * 往广播状态中放
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleStateBean {
    private MarketingRule marketingRule; // json
    private KieSession kieSession; // drl controller
}
