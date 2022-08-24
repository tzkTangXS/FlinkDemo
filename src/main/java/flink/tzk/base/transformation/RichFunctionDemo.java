package flink.tzk.base.transformation;

import flink.tzk.bean.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<Event> eventStream = env.fromElements(new Event("jack", "//home", 1000L),
                new Event("bob", "//home/login", 1200L),
                new Event("jack","//home/id?",2000L),
                new Event("jack","//home/chart?chartId=313214,id=2",3200L),
                new Event("bob","//home?id=20",2200L));
        SingleOutputStreamOperator<Long> map = eventStream.map(new MyRichMapper()).setParallelism(2);
        map.printToErr();
        //1.shuffle分区:随机发牌
//        map.shuffle().print().setParallelism(2);
        //2.轮询一条龙服务分区
//        map.rebalance();
        //3. 重缩放分区，自己小组类进行分区。
//        map.rescale();
//        4. 广播分区
//        map.broadcast().print().setParallelism(3);
//        全局分区
//        map.global().print().setParallelism(4);
//        6.自定义分区
        map.partitionCustom(new Partitioner<Long>() {
            @Override
            public int partition(Long aLong, int i) {
                return (int) (aLong%2);
            }
        }, new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long aLong) throws Exception {
                return aLong/2;
            }
        }).print().setParallelism(5);

        env.execute();

    }
    public static class MyRichMapper extends RichMapFunction<Event,Long> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open:\t"+getRuntimeContext().getTaskName()+getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close:\t"+getRuntimeContext().getTaskName()+getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Long map(Event event) throws Exception {
            return event.timestamp;
        }

    }
}
