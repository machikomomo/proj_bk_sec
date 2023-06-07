package cn.odyssey.fact.flink_demo;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 把一个.drl文件里的内容，作为字符串，写到mysql数据库的rule_demo这张表里
 */
public class RuleInjector {
    public static void main(String[] args) throws Exception {
        String drlString = FileUtils.readFileToString(new File("/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/rule_engine/src/main/resources/rules/demo4.drl"), "utf-8");
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink_dynamic?useUnicode=true&characterEncoding=utf8", "root", "000000");
        PreparedStatement preparedStatement = connection.prepareStatement("insert into rule_demo(rule_name,drl_string,online) values(?,?,?)");
        preparedStatement.setString(1, "demo4drlNew");
        preparedStatement.setString(2, drlString);
        preparedStatement.setString(3, "1");
        preparedStatement.execute();
        preparedStatement.close();
        connection.close();
    }
}
