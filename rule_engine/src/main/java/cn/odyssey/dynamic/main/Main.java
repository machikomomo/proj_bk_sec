package cn.odyssey.dynamic.main;

import cn.odyssey.dynamic.beans.CanalBinlogBean;
import cn.odyssey.dynamic.beans.LogBean;
import cn.odyssey.dynamic.beans.RuleMatchResult;
import cn.odyssey.dynamic.beans.RuleStateBean;
import cn.odyssey.dynamic.functions.KafkaSourceBuilder;
import cn.odyssey.dynamic.functions.NewRuleMatchKeyedProcessFunction;
import cn.odyssey.dynamic.functions.RuleMatchKeyedProcessFunction;
import cn.odyssey.dynamic.utils.ConfigNames;
import cn.odyssey.dynamic.utils.StateDescContainer;
import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // logBean数据流
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
        // keyed logBean数据流


        // canalBean数据流
        // 读取kafka中的规则操作数据流canal binlog
        DataStreamSource<String> canalBinlogDSS = env.addSource(new KafkaSourceBuilder().build("rule-demo"));
        SingleOutputStreamOperator<CanalBinlogBean> canalBeanDSS = canalBinlogDSS.map(new MapFunction<String, CanalBinlogBean>() {
            @Override
            public CanalBinlogBean map(String s) throws Exception {
                return JSON.parseObject(s, CanalBinlogBean.class);
            }
        });

        // canalBean数据流
        // 这个状态要再写一下
        MapStateDescriptor<String, RuleStateBean> mapStateDescriptor = StateDescContainer.MapStateDescriptor;
        BroadcastStream<CanalBinlogBean> broadcast = canalBeanDSS.broadcast(mapStateDescriptor);

        // TODO 连接，数据流connect广播流
        BroadcastConnectedStream<LogBean, CanalBinlogBean> connect = keyed.connect(broadcast);

        // TODO 现核心计算逻辑
        SingleOutputStreamOperator<RuleMatchResult> processNew = connect.process(new NewRuleMatchKeyedProcessFunction());


        // process 原核心计算逻辑
//        SingleOutputStreamOperator<RuleMatchResult> process = keyed.process(new RuleMatchKeyedProcessFunction());

//        process.print();

        processNew.print();
        env.execute();
    }
}
