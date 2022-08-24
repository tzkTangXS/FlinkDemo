package flink.tzk;




import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;


import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;



public class WordCount {
    public static void main(String[] args) throws Exception {
/*        *//*
         * 创建环境
         * *//*
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        *//**
         * 得到数据
         * *//*
        DataSource<String> lineSource = env.readTextFile("/input/text.txt");
        *//*
        * 处理数据逻辑*//*
        FlatMapOperator<String, Object> wordCounts = lineSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        UnsortedGrouping<Object> objectUnsortedGrouping = wordCounts.groupBy(0);
        AggregateOperator<Object> sum = objectUnsortedGrouping.sum(1);
        sum.print();*/

    }
}
