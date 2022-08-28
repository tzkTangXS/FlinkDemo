package flink.tzk.base.window;



import flink.tzk.bean.Event;
import flink.tzk.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.lang.model.element.ElementVisitor;
import java.time.Duration;

public class TumblingWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
/*        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("hostName");
        Integer port = new Integer(parameterTool.get("port"));
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(hostName, port);*/
/*        DataStream<Event> eventStream = env.fromElements(new Event("jack", "//home", 10000L),
                new Event("bob", "//home/login", 10000L),
                new Event("jack","//home/id?",10000L),
                new Event("jack","//home/chart?chartId=313214,id=2",10000L),
                new Event("bob","//home?id=20",2200000L));*/
        DataStream<Event> eventStream = env.addSource(new ClickSource());
        //水位线
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
        //窗口函数
        SingleOutputStreamOperator<Tuple2<String, Long>> wordCountWindow = eventSingleOutputStreamOperator
                .map(x -> Tuple2.of(x, 1L)).returns(Types.TUPLE(TypeInformation.of(Event.class), Types.LONG))
                .keyBy(kv -> kv.f0.user)
//               .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                .reduce((x, y) -> Tuple2.of(x.f0, (x.f1 + y.f1)));
.aggregate(new AggregateFunction<Tuple2<Event, Long>, Tuple2<String,Long>, Tuple2<String, Long>>() {
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return Tuple2.of("",0L);
    }

    @Override
    public Tuple2<String, Long> add(Tuple2<Event, Long> eventLongTuple2, Tuple2<String, Long> stringLongTuple2) {
        return Tuple2.of(eventLongTuple2.f0.user,eventLongTuple2.f1+stringLongTuple2.f1);
    }

    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
        return stringLongTuple2;
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
        return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1+acc1.f1);
    }
});
        wordCountWindow.print();
        env.execute();
    }
}
