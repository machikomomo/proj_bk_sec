import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 来一条事件，打印事件的时间
 */

public class Test07Watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dss = env.socketTextStream("localhost", 5656);

        SingleOutputStreamOperator<String> watermarked = dss.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return Long.parseLong(s.split(",")[2]);
            }
        }));

        KeyedStream<String, String> keyed = watermarked.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s.split(",")[0];
            }
        });

        SingleOutputStreamOperator<String> processed = keyed.process(new KeyedProcessFunction<String, String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                System.out.println("context.timestamp() = " + context.timestamp()); // 这个context.timestamp就是当前事件的时间戳
                System.out.println("watermark = " + context.timerService().currentWatermark());
                System.out.println("current process time " + context.timerService().currentProcessingTime());
            }
        });
        processed.print();
        env.execute();

    }
}
