import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class Test12FlinkMetric {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> dss = env.socketTextStream("localhost", 5656);
        SingleOutputStreamOperator<String> processed = dss.process(new ProcessFunction<String, String>() {

            Counter process_call_count;
            Counter process_call_time_amount;
            MeterView process_call_count_per_second;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 定义2个度量组
                MetricGroup g1 = getRuntimeContext().getMetricGroup().addGroup("g1");
                MetricGroup g2 = getRuntimeContext().getMetricGroup().addGroup("g2");

                // g1组中定义2个度量
                process_call_count = g1.counter("process_call_count");
                process_call_time_amount = g1.counter("process_call_time_amount");

                // g2组中定义1个度量
                process_call_count_per_second = g2.meter("process_call_count_per_second", new MeterView(process_call_count, 5));

            }

            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                long start = System.currentTimeMillis();
                Thread.sleep(RandomUtils.nextInt(500, 5000));
                long end = System.currentTimeMillis();
                process_call_count.inc(1);
                process_call_time_amount.inc(end - start);
                process_call_count_per_second.markEvent();
                String ss = s.toUpperCase();
                collector.collect(ss);
            }
        });
        processed.print();
        env.execute();
    }
}
