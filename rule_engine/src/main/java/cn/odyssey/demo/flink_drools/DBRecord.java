package cn.odyssey.demo.flink_drools;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DBRecord {
    private String id;
    private String rule_name;
    private String drl_string;
    private String online;
}
