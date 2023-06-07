package cn.odyssey.fact;

import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;

/**
 * 后续参考这个模版
 */
public class DroolsDemo3 {
    public static void main(String[] args) throws IOException {
        String drlString = FileUtils.readFileToString(new File("/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/rule_engine/src/main/resources/rules/demo2.drl"),"utf-8");
//        System.out.println(drlString);
        // 模版
        KieHelper kieHelper = new KieHelper();
        kieHelper.addContent(drlString, ResourceType.DRL);
        KieSession kieSession = kieHelper.build().newKieSession();
        // 模版
        Student student = new Student(18);
        Teacher teacher = new Teacher(28);

        kieSession.insert(student);
        kieSession.insert(teacher);
        kieSession.fireAllRules();
        kieSession.dispose();

        int age = student.getAge();
        System.out.println(age);
    }
}
