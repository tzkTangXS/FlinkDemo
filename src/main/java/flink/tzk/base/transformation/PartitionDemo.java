package flink.tzk.base.transformation;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2);
        integerDataStreamSource.print();
        integerDataStreamSource.rescale().printToErr().setParallelism(4);
        integerDataStreamSource.map(x-> Tuple2.of(x,1)).returns(Types.TUPLE(Types.INT,Types.INT)).rescale().writeAsCsv("input/test12.csv").setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource1 = env.fromElements(1, 2, 3, 4, 5, 56, 6);


        env.execute();
    }
}
