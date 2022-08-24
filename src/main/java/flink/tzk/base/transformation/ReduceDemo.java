package flink.tzk.base.transformation;

import flink.tzk.bean.Event;
import org.apache.commons.math3.analysis.function.Max;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Collector;

public class ReduceDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<Event> eventStream = env.fromElements(new Event("jack", "//home", 1000L),
                new Event("bob", "//home/login", 1200L),
                new Event("jack","//home/id?",2000L),
                new Event("jack","//home/chart?chartId=313214,id=2",3200L),
                new Event("bob","//home?id=20",2200L));
        SingleOutputStreamOperator<Tuple2<String, Integer>> userCountTuple2 = eventStream.map(x -> Tuple2.of(x.user, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> userCont = userCountTuple2.keyBy(kv -> kv.f0).reduce((x, y) -> Tuple2.of(x.f0, x.f1 + y.f1));
        userCont.print("reduce:");
        SingleOutputStreamOperator<Tuple2<String, Integer>> max = userCont.keyBy(kv -> "key").reduce((x, y) -> x.f1 > y.f1 ? x : y);
        max.print("lastReduce:");
        env.execute();
    }
}
