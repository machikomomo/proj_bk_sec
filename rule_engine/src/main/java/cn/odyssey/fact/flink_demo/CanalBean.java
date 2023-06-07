package cn.odyssey.fact.flink_demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CanalBean {
    private List<DBRecord> data;
    private String type;
}
