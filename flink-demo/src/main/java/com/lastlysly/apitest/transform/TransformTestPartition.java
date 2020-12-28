package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-28 12:34
 * 重分区
 **/
public class TransformTestPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());

        inputStream.print();

        // 1，shuffer 重分区
        DataStream<SensorReading> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // 2, keyby  分组后分区 会发现同一组一直在同一个分区
        DataStream<SensorReading> keyByStream = inputStream.keyBy(SensorReading::getId);

        keyByStream.print("keyBy");

        // 3, global 全部分配到下游第一个分区

        DataStream<SensorReading> globalStream = inputStream.global();
        globalStream.print("global");

        env.execute();
    }
}
