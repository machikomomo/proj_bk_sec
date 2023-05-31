package cn.odyssey.engine.entry;

import cn.odyssey.beans.LogBean;
import cn.odyssey.beans.RuleMatchResult;
import cn.odyssey.engine.functions.KafkaSourceBuilder;
import cn.odyssey.engine.functions.RuleMatchKeyedProcessFunction;
import cn.odyssey.engine.utils.ConfigNames;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 需求：获得用户事件，判断如下需求，返回结果
 * 触发条件：K事件 事件属性（p2=v1）
 * 画像属性条件：  tag87=v2 , tag26=v1
 * 行为次数条件：  2021-6-18 ～ 当前 事件C（p6=v8，p12=v5）做过>=2次
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 读取kafka中的用户行为日志
        String topic = "detail_action_log";
        DataStreamSource<String> dss = env.addSource(new KafkaSourceBuilder().build(topic));

        // 把string转为logBean流
        SingleOutputStreamOperator<LogBean> beanSS = dss.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String s) throws Exception {
                return JSON.parseObject(s, LogBean.class);
            }
        });

        // keyBy deviceId
        KeyedStream<LogBean, String> keyed = beanSS.keyBy(new KeySelector<LogBean, String>() {
            @Override
            public String getKey(LogBean logBean) throws Exception {
                return logBean.getDeviceId();
            }
        });

        // process 核心计算逻辑
        SingleOutputStreamOperator<RuleMatchResult> process = keyed.process(new RuleMatchKeyedProcessFunction());

        process.print();

        env.execute();
    }
}
