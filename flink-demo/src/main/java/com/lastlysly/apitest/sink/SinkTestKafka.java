package com.lastlysly.apitest.sink;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-28 17:15
 **/
public class SinkTestKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSourceStream());
        DataStream<String> dataStream = inputStream.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                sensorReading.setId(sensorReading.getId() + "_test");
                return sensorReading.toString();
            }
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("192.168.56.18:9092","lastlyslytest",new SimpleStringSchema()));

        env.execute();
    }
}
