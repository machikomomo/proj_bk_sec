package cn.odyssey.marketing.main;

import cn.odyssey.marketing.beans.LogBean;
import cn.odyssey.marketing.beans.RuleMatchResult;
import cn.odyssey.marketing.functions.KafkaSourceBuilder;
import cn.odyssey.marketing.functions.RuleMatchKeyedProcessFunction;
import cn.odyssey.marketing.utils.ConfigNames;
import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 读取kafka中的用户行为日志
        String topic = config.getString(ConfigNames.KAFKA_ACTION_DETAIL_TOPIC);
        DataStreamSource<String> dss = env.addSource(new KafkaSourceBuilder().build(topic));

        // 把string转为logBean流
        SingleOutputStreamOperator<LogBean> beanSS = dss.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String s) throws Exception {
                return JSON.parseObject(s, LogBean.class);
            }
        });

        // 添加事件时间分配
        WatermarkStrategy<LogBean> logBeanWatermarkStrategy = WatermarkStrategy.<LogBean>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner(new SerializableTimestampAssigner<LogBean>() {
            @Override
            public long extractTimestamp(LogBean logBean, long l) {
                return logBean.getTimeStamp();
            }
        });

        SingleOutputStreamOperator<LogBean> withWatermarkAndTimestamp = beanSS.assignTimestampsAndWatermarks(logBeanWatermarkStrategy);

        // keyBy deviceId
        KeyedStream<LogBean, String> keyed = withWatermarkAndTimestamp.keyBy(new KeySelector<LogBean, String>() {
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
