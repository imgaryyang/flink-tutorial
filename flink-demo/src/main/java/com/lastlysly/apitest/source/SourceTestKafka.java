package com.lastlysly.apitest.source;

import com.lastlysly.demo.WordBatchCountDemo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-16 17:17
 * 从 kafka 中读取数据 (注意开启kafka的远程连接)
 **/
public class SourceTestKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.56.18:9092");
        // 从 kafka读取数据
        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer<String>("lastlyslytest",new SimpleStringSchema(),properties));
        DataStream<Tuple2<String, Integer>> outputStream = dataStream.flatMap(
                new WordBatchCountDemo.MyFlatMapper()).setParallelism(2)
                .keyBy(0)
                .sum(1);
//        dataStream.print();
        outputStream.print();
        env.execute();
    }
}
