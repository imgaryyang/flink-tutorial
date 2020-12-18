package com.lastlysly.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-14 17:24
 *
 * 流处理 word count
 **/
public class WordStreamCount {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 设置并行度
//            env.setParallelism(3);
//            String inputPath = "F:\\JetBrains\\IntelliJ IDEA\\learningspace\\flink-tutorial\\flink-demo\\src\\main\\resources\\files\\hello.txt";
//            System.out.println(inputPath);
//            DataStream<String> inputData = env.readTextFile(inputPath);

            // 用parameter tool 工具从程序启动参数中提取配置项，运行jar添加运行参数，用于生成环境的提交
//            ParameterTool parameterTool = ParameterTool.fromArgs(args);
//            String host = parameterTool.get("host");
//            int port = parameterTool.getInt("port");


            // 从socket文本流获取数据
//            DataStream<String> inputData = env.socketTextStream(host,port);
            DataStream<String> inputData = env.socketTextStream("localhost",8082);


            // 基于数据流进行转换计算
            DataStream<Tuple2<String,Integer>> result = inputData.flatMap(new WordBatchCountDemo.MyFlatMapper())
                    .keyBy(0)
                    .sum(1);
            result.print();

            // 执行任务
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
