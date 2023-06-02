import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * 1,A,1677777771000（注册定时器）
 * 1,B,1677777771050
 * 1,B,1677777772000
 * 1,B,1677777774000
 * 1,B,1677777775000 （执行到这里，小世界里面的时间就是1677777775000，也就是水位线已经跑到了这里，小世界里的时间推进到了这里，也比第一条A超出了3s）
 *
 * 1,C,1677777776000
 * 1,A,1677777776100
 * 1,C,1677777776200
 * 1,B,1677777779999
 *
 * （模拟迟到）
 * 1,A,1677777780000
 * 1,C,1677777780100
 * 1,B,1677777789999
 *
 * 做了A后（事件发生的时间），3秒内没有做C，输出一条消息
 */
public class Test06OnTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> dss = env.socketTextStream("localhost", 5656); // 并行度只能为1

        SingleOutputStreamOperator<String> watermarked = dss.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return Long.parseLong(s.split(",")[2]);
            }
        })); // 分配了时间水印

        KeyedStream<String, String> keyed = watermarked.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s.split(",")[0];
            }
        });

        SingleOutputStreamOperator<String> processed = keyed.process(new KeyedProcessFunction<String, String, String>() {

            ListState<Tuple2<String, Long>> tmpState;

            @Override
            public void open(Configuration parameters) throws Exception {

                tmpState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("tmpState", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })));

            }

            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {

                String[] arr = s.split(",");
                if ("A".equals(arr[1])) {
                    // 注册一个定时器
                    context.timerService().registerEventTimeTimer(Long.parseLong(arr[2]) + 3000); // 3s
                }
                if ("C".equals(arr[1])) {
                    tmpState.add(Tuple2.of("C", Long.parseLong(arr[2])));
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                boolean flag = true;
                Iterable<Tuple2<String, Long>> iterable = tmpState.get();
                Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Long> next = iterator.next();
                    if (next.f1 < timestamp) {
                        flag = false;
                        iterator.remove(); // 这个元素属于某个A，所以移除
                    }
                }
                if (flag) {
                    out.collect(ctx.getCurrentKey() + "在做了A以后3秒内，没做C");
                }
            }
        });
        processed.print();
        env.execute();
    }
}
