package cn.odyssey.dynamic.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CanalBinlogBean {
    private List<BinlogDataRecord> data;
    private String type;

}
