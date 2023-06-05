package cn.odyssey.demo.flink_drools;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class RuleInjector {
    public static void main(String[] args) throws Exception {
        String drlStr = FileUtils.readFileToString(new File("/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/rule_engine/src/main/resources/rules/demo2.drl"), "utf-8");
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink_dynamic?useUnicode=true&characterEncoding=utf8", "root", "000000");
        PreparedStatement preparedStatement = conn.prepareStatement("insert into rule_demo(rule_name,drl_string,online) values(?,?,?)");
        preparedStatement.setString(1, "demo2");
        preparedStatement.setString(2, drlStr);
        preparedStatement.setString(3, "1");
        preparedStatement.execute();
        preparedStatement.close();
        conn.close();
    }
}
