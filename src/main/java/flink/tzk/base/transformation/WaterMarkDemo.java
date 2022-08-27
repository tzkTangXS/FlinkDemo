package flink.tzk.base.transformation;

import flink.tzk.bean.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WaterMarkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置周期waterMark的触发生成机制的周期
        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);
        DataStream<Event> eventStream = env.fromElements(new Event("jack", "//home", 1000L),
                new Event("bob", "//home/login", 1200L),
                new Event("jack","//home/id?",2000L),
                new Event("jack","//home/chart?chartId=313214,id=2",3200L),
                new Event("bob","//home?id=20",2200L));
/*        eventStream.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return null;
            }

            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return null;
            }
        });*/
        eventStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
    }
}
