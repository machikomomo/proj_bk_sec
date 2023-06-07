package cn.odyssey.fact;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import java.util.Arrays;

public class DroolsDemo {
    public static void main(String[] args) {
        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieClasspathContainer = kieServices.getKieClasspathContainer();
        KieSession kieSession = kieClasspathContainer.newKieSession("s1");

        // fact
        Person person = new Person("yahaha", "manager", 25, "male", Arrays.asList("jack"), new Cat("miki", "orange"));
        Person person2 = new Person("miehaha", "manager", 18, "female", Arrays.asList("jack", "mike"), new Cat("miki", "orange"));

        kieSession.insert(person);
        kieSession.insert(person2);
        kieSession.fireAllRules();
        kieSession.dispose();
    }
}
