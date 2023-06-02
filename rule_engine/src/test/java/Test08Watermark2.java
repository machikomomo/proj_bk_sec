import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *  * 1,A,1677777771000
 *  * 2,B,1677777771050 2条结束同步到 1677777770999
 *  * 1,B,1677777772000 2条结束同步到 1677777771049
 *  * 2,B,1677777774000 2条结束同步到 1677777771999
 *  * 1,B,1677777775000
 */
public class Test08Watermark2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 2条才同步一次水位
        env.setParallelism(2);
        DataStreamSource<String> dss = env.socketTextStream("localhost", 5656); // 并行度只能为1
        SingleOutputStreamOperator<Tuple3<String, String, Long>> mapped = dss.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Long>> watermarked = mapped.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                return stringStringLongTuple3.f2;
            }
        }));

        KeyedStream<Tuple3<String, String, Long>, String> keyed = watermarked.keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return stringStringLongTuple3.f0;
            }
        });

        SingleOutputStreamOperator<String> processed = keyed.process(new KeyedProcessFunction<String, Tuple3<String, String, Long>, String>() {
            @Override
            public void processElement(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
                //
                System.out.println("context.timestamp() = " + context.timestamp()); // 这个context.timestamp就是当前事件的时间戳
                System.out.println("watermark = " + context.timerService().currentWatermark());
                System.out.println("current process time " + context.timerService().currentProcessingTime());
            }
        });
        processed.print();
        env.execute();
    }
}
