package cn.odyssey.demo.flink_drools;

import cn.odyssey.dynamic.functions.KafkaSourceBuilder;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.List;
import java.util.Map;

/**
 * flink整合drools demo
 */
@Slf4j
public class FlinkDrools {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        // 读取业务数据
        DataStreamSource<String> dss = env.socketTextStream("localhost", 5656);
        SingleOutputStreamOperator<DataBean> mappedDss = dss.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String s) throws Exception {
                return new DataBean(s, null);
            }
        });

        // 读取规则流
        DataStreamSource<String> drlStream = env.addSource(new KafkaSourceBuilder().build("rule-demo"));

        MapStateDescriptor<String, KieSession> stringStringMapStateDescriptor = new MapStateDescriptor<>("rule-state", String.class, KieSession.class);
        BroadcastStream<String> broadcast = drlStream.broadcast(stringStringMapStateDescriptor);
        BroadcastConnectedStream<DataBean, String> connect = mappedDss.connect(broadcast);
        connect.process(new BroadcastProcessFunction<DataBean, String, String>() {
            @Override
            public void processElement(DataBean dataBean, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, KieSession> broadcastState = readOnlyContext.getBroadcastState(stringStringMapStateDescriptor);
                Iterable<Map.Entry<String, KieSession>> entries = broadcastState.immutableEntries();
                for (Map.Entry<String, KieSession> entry : entries) {
                    // TODO 调用drools引擎，对进来的业务数据data进行处理
                    KieSession kieSession = entry.getValue();
                    kieSession.insert(dataBean);
                    kieSession.fireAllRules();

                    // TODO 输出处理结果
                    collector.collect(dataBean.getRes());
                }
            }

            @Override
            public void processBroadcastElement(String canalBinlog, Context context, Collector<String> collector) throws Exception {
                CanalBean canalBean = JSON.parseObject(canalBinlog, CanalBean.class);
                BroadcastState<String, KieSession> broadcastState = context.getBroadcastState(stringStringMapStateDescriptor);
                List<DBRecord> data = canalBean.getData();
                for (DBRecord dbRecord : data) {
                    String rule_name = dbRecord.getRule_name();
                    String drl_string = dbRecord.getDrl_string();
                    // 将drl规则字符串，构造成KieSession
                    KieHelper kieHelper = new KieHelper();
                    kieHelper.addContent(drl_string, ResourceType.DRL);
                    KieSession kieSession = kieHelper.build().newKieSession();

                    String type = canalBean.getType();
                    String online = dbRecord.getOnline();

                    if ("INSERT".equals(type) || "UPDATE".equals(type) && "1".equals(online)) {
                        broadcastState.put(rule_name, kieSession);
                        log.debug("fuck insert");
                    } else if ("DELETE".equals(type) || ("UPDATE".equals(type) && "0".equals(online))) {
                        broadcastState.remove(rule_name);
                        log.debug("fuck del");
                    }
                }

            }
        }).print();
        env.execute();
    }
}
