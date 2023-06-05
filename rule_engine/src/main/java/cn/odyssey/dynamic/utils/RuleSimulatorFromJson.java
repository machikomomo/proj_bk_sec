package cn.odyssey.dynamic.utils;

import cn.odyssey.dynamic.beans.MarketingRule;
import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RuleSimulatorFromJson {
    public static List<MarketingRule> getRule() throws IOException {
        String s = FileUtils.readFileToString(new File("rule_engine/rules/rule1.json"), "utf-8");
        MarketingRule marketingRule = JSON.parseObject(s, MarketingRule.class);

        String s2 = FileUtils.readFileToString(new File("rule_engine/rules/rule2.json"), "utf-8");
        MarketingRule marketingRule2 = JSON.parseObject(s2, MarketingRule.class);
        return Arrays.asList(marketingRule, marketingRule2);
    }
}
