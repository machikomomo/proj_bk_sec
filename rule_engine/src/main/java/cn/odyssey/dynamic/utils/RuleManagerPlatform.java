package cn.odyssey.dynamic.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 模拟器 用于数据库数据增删改
 */
public class RuleManagerPlatform {

    public static void main(String[] args) throws Exception {
        insertRule();
    }

    /**
     * 插入新规则到mysql
     */

    public static void insertRule() throws Exception {
        String jsonStr = FileUtils.readFileToString(new File("rule_engine/rules/rule1.json"), "utf-8");
        String drlStr = FileUtils.readFileToString(new File("rule_engine/rules/rule_dynamic2.drl"), "utf-8");
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink_dynamic?useUnicode=true&characterEncoding=utf8", "root", "000000");
        PreparedStatement preparedStatement = conn.prepareStatement("insert into rule_demo(rule_name,rule_condition_json,rule_controller_drl,rule_status,create_time,modify_time,author) values(?,?,?,?,?,?,?)");
        preparedStatement.setString(1, "rule_dynamic2");
        preparedStatement.setString(2, jsonStr); // 原来的规则json
        preparedStatement.setString(3, drlStr); // drl文件，充当原来的controller角色
        preparedStatement.setString(4, "1"); // 生效
        preparedStatement.setString(5, "2023-06-07 12:30:00");
        preparedStatement.setString(6, "2023-06-07 12:30:00");
        preparedStatement.setString(7, "momoka");
        preparedStatement.execute();
        preparedStatement.close();
        conn.close();
    }

    /**
     * 更新已存在的规则
     */

    /**
     * 删除
     */

    /**
     * 启动
     */

    /**
     * 停用
     */
}
