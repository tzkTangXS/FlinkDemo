package flink.tzk.base;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        //得到流
        DataStreamSource<String> lineDataStreamSource = env.socketTextStream(hostname, port);
//        操作逻辑
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words){
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2SingleOutputStreamOperator.keyBy(kv -> kv.f0).countWindow(3).sum(1).setParallelism(20);
        sum.print();
        env.execute();
    }
}
