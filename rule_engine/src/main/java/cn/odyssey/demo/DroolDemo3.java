package cn.odyssey.demo;

import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;

public class DroolDemo3 {
    public static void main(String[] args) throws IOException {
        String drlStr = FileUtils.readFileToString(new File("/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/rule_engine/src/main/resources/rules/rule2.drl"), "utf-8");
        // 用规则字符串来构造一个kieSession
        KieHelper kieHelper = new KieHelper();
        kieHelper.addContent(drlStr, ResourceType.DRL);
        KieSession kieSession = kieHelper.build().newKieSession();

        // 调用规则引擎
        Student student = new Student(38);
        Teacher teacher = new Teacher(40);

        kieSession.insert(student);
        kieSession.insert(teacher);
        kieSession.fireAllRules();
        kieSession.dispose();

        System.out.println("年龄之和"+student.getAge());

    }
}
