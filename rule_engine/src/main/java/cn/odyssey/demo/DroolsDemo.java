package cn.odyssey.demo;


import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import java.util.Arrays;

public class DroolsDemo {
    public static void main(String[] args) {

        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieClasspathContainer = kieServices.getKieClasspathContainer();
        KieSession kieSession = kieClasspathContainer.newKieSession("s1");

        // 构造fact对象，插入到规则引擎
        Person person = new Person("zhangsan", "manager", 32, "male", Arrays.asList("jack", "tom"), new Cat("kiki", "orange"));

        kieSession.insert(person);
        kieSession.fireAllRules();

        // 销毁会话
        kieSession.dispose();

    }
}
