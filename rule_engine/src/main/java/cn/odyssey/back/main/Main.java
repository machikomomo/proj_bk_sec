package cn.odyssey.back.main;

import cn.odyssey.back.beans.LogBean;
import cn.odyssey.back.beans.RuleMatchResult;
import cn.odyssey.back.functions.KafkaSourceBuilder;
import cn.odyssey.back.functions.RuleMatchKeyedProcessFunctionV3;
import cn.odyssey.back.utils.ConfigNames;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

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

        // 得到LogBean流以后，添加事件时间分配
        WatermarkStrategy<LogBean> logBeanWatermarkStrategy = WatermarkStrategy.<LogBean>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner(new SerializableTimestampAssigner<LogBean>() {
            @Override
            public long extractTimestamp(LogBean logBean, long l) {
                return logBean.getTimeStamp();
            }
        });
        SingleOutputStreamOperator<LogBean> watermarked = beanSS.assignTimestampsAndWatermarks(logBeanWatermarkStrategy);

        // key by deviceId
        KeyedStream<LogBean, String> keyed = watermarked.keyBy(new KeySelector<LogBean, String>() {
            @Override
            public String getKey(LogBean logBean) throws Exception {
                return logBean.getDeviceId();
            }
        });

        // keyed process function
        SingleOutputStreamOperator<RuleMatchResult> processed = keyed.process(new RuleMatchKeyedProcessFunctionV3());

        processed.print();

        env.execute();
    }
}
