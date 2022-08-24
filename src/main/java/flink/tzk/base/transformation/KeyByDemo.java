package flink.tzk.base.transformation;

import flink.tzk.bean.Event;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {
    public static void main(String[] args) throws Exception {

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventStream = env.fromElements(new Event("jack", "//home", 1000L),
                new Event("bob", "//home/login", 1200L),
                new Event("jack","//home/id?",2000L),
                new Event("jack","//home/chart?chartId=313214,id=2",3200L),
                new Event("bob","//home?id=20",2200L));
        eventStream.keyBy(kv -> kv.user).max("timestamp").print("max:\t");
        eventStream.keyBy(kv -> kv.user).maxBy("timestamp").print("maxBy:\t");
        System.out.println("-----------------------------");
        env.execute();


    }
}
