package cn.odyssey.demo;


import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import java.util.Arrays;

public class DroolsDemo2 {
    public static void main(String[] args) {

        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieClasspathContainer = kieServices.getKieClasspathContainer();
        KieSession kieSession = kieClasspathContainer.newKieSession("s1");

        // 构造fact对象，插入到规则引擎
        Student student = new Student(18);
        Teacher teacher = new Teacher(40);


        kieSession.insert(student);
        kieSession.insert(teacher);
        kieSession.fireAllRules();

        // 销毁会话
        kieSession.dispose();

        System.out.println(student.getAge());

    }
}
