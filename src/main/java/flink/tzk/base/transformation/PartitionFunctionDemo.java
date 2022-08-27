package flink.tzk.base.transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionFunctionDemo {
    public static void main(String[] args) throws Exception {
        //环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据
        DataStreamSource<String> stringDataStreamSource = env.fromElements("小明", "大海", "王五", "咳咳", "哀切");
        stringDataStreamSource.map(x -> Tuple2.of(x,1L)).partitionCustom(new Partitioner<Long>() {
            @Override
            public int partition(Long aLong, int i) {
                return (int) (aLong%2);
            }
        }, new KeySelector<Tuple2<String,Long>, Long>() {
            @Override
            public Long getKey(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f1;
            }
        });

        /**
         * 分区方法
         * */
        //shuffle，打乱随机均匀分配
//        stringDataStreamSource.shuffle().print().setParallelism(3);
        // rebalance，轮询均匀分配。算法：Round-Robin
//        stringDataStreamSource.rebalance().print().setParallelism(3);
        //rescala,内部轮询均匀分配，（之后在上游任务和下游对应的部分任务之间建立通信），通过合理的设定slot，从而避免进行网络io。算法：Round-Robin
        stringDataStreamSource.rescale().print().setParallelism(3);
        //执行环境
        env.execute();

    }
}
