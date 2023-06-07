package cn.odyssey.fact.flink_demo;

import cn.odyssey.dynamic.functions.KafkaSourceBuilder;
import com.alibaba.fastjson.JSON;
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
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.List;
import java.util.Map;

/**
 * 数据流 是数字就*100， 否则转大写
 */
public class FlinkDroolsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 数据流
        DataStreamSource<String> dataDSS = env.socketTextStream("localhost", 6918);
        SingleOutputStreamOperator<DataBean> mapped = dataDSS.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String s) throws Exception {
                return new DataBean(s, null);
            }
        });

        // 规则流
        DataStreamSource<String> ruleDSS = env.addSource(new KafkaSourceBuilder().build("rule-demo"));
        // new一个广播状态放key：rule_name，value：kisSession
        MapStateDescriptor<String, KieSession> mapStateDescriptor = new MapStateDescriptor<>("rule", String.class, KieSession.class);
        BroadcastStream<String> broadcastStream = ruleDSS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<DataBean, String> connect = mapped.connect(broadcastStream);

        connect.process(new BroadcastProcessFunction<DataBean, String, String>() {

            @Override
            public void processElement(DataBean dataBean, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, KieSession> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                Iterable<Map.Entry<String, KieSession>> entries = broadcastState.immutableEntries();
                for (Map.Entry<String, KieSession> entry : entries) {
                    KieSession kieSession = entry.getValue();
                    kieSession.insert(dataBean);
                    kieSession.fireAllRules();
                    collector.collect(dataBean.getRes());
                }
            }

            @Override
            public void processBroadcastElement(String canalBinlog, Context context, Collector<String> collector) throws Exception {
                // 后续把xxx放进状态描述器里
                BroadcastState<String, KieSession> broadcastState = context.getBroadcastState(mapStateDescriptor);
                // 广播流数据转为canal bean 后续操作方便
                CanalBean canalBean = JSON.parseObject(canalBinlog, CanalBean.class);
                List<DBRecord> dbRecordList = canalBean.getData();
                for (DBRecord dbRecord : dbRecordList) {
                    String rule_name = dbRecord.getRule_name();
                    String drl_string = dbRecord.getDrl_string();
                    String online = dbRecord.getOnline();

                    // drl_string to kieSession 模版
                    KieHelper kieHelper = new KieHelper();
                    kieHelper.addContent(drl_string, ResourceType.DRL);
                    KieSession kieSession = kieHelper.build().newKieSession();

                    // kieSession to state
                    // 判断变更是什么操作 目的，后续处理广播流状态；判断依据，既要看canalType又要看online
                    String canalType = canalBean.getType();
                    // 那insert时，也就是第一次放入规则时，online必须为1
                    if ("INSERT".equals(canalType) || ("UPDATE".equals(canalType) && "1".equals(online))) {
                        broadcastState.put(rule_name, kieSession);
                    } else if ("DELETE".equals(canalType) || ("UPDATE".equals(canalType) && "0".equals(online))) {
                        broadcastState.remove(rule_name);
                    }
                }
            }
        }).print();
        env.execute();
    }
}
