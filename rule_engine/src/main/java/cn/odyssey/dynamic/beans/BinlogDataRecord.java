package cn.odyssey.dynamic.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * flink要用的字段只有rule_name、rule_condition_json、rule_controller_drl、rule_status；其他都是为了源数据存储服务
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BinlogDataRecord {
    private int id;
    private String rule_name;
    private String rule_condition_json;
    private String rule_controller_drl;
    private String rule_status;
    private String create_time;
    private String modify_time;
    private String author;
}
