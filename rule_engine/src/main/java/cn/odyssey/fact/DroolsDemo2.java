package cn.odyssey.fact;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class DroolsDemo2 {
    public static void main(String[] args) {
        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieClasspathContainer = kieServices.getKieClasspathContainer();
        KieSession kieSession = kieClasspathContainer.newKieSession("s1");
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
