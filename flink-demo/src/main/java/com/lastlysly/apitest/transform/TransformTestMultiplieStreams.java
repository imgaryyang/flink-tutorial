package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-22 09:31
 * 多流转换  split / select   过时的API  使用  side output instead. 替代，为底层API，后续Demo会提到
 * DataStream 的 api。无需keyBy 即可调用
 *
 * 1，分流：split（分流） => SplitStream => select（取流，根据分流分出的标签名）
 * 2，合流： connect  一次只能合两条流，两条流 的类型可以不一样
 * 3，合流： union      一次可以合多条流，流的类型必须一样
 **/
public class TransformTestMultiplieStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                sensorReading.setId(sensorReading.getId() + "多流");
                return sensorReading;
            }
        });

//        KeyedStream<SensorReading,String> keyedStream = dataStream.keyBy(SensorReading::getId);

        /**
         * 1，分流 split  例子：按照 温度值50度为界，分为高温低温两条流
         */
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
//                return value.getTemperature() > 50 ? Arrays.asList("high") : Arrays.asList("low");
                return value.getTemperature() > 50 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        /**
         * SplitStream 自实现 的方法  select 。传入之前分流的标签名 如： high，low等
         */
        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> highAndLowTempStream = splitStream.select("low","high");
        highTempStream.print("highTempStream");
        lowTempStream.print("lowTempStream");
        highAndLowTempStream.print("highAndLowTempStream");


        /**
         * 2，合流 connect，合流可以合并 两个类型不同的流，这里为了突出该功能，
         * 我们将将高温流转为 二元组，即让其与低温流数据类型不一样
         *
         *  与低温流连接合并后输出状态信息
         *
         */
        DataStream<Tuple2<String, Double>> mapHighStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });
        // 合流，返回ConnectedStreams，map,flatmap等操作实现发生变化，如 map 中为 CoMapFunction
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = mapHighStream.connect(lowTempStream);
        // 合流后 统一输出 格式，三个 入参，
        // 合流时调用者类型 为 这里的第一个入参类型， 为参数者类型 为 这里的第二个参数类型
        // 最后一个为合流后的结果的类型，可以和两个入参类型都不一样，这里以输出三元组为例。

        DataStream<Tuple3<String, Double, String>> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "高温报警了");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReading value) throws Exception {
                return new Tuple3<>(value.getId(), value.getTemperature(), "低温流");
            }
        });

        resultStream.print("connect合流并转换为统一类型后输出");


        /**
         * 3，合流： union   一次可以合多条流，流的类型必须一样
         *      如，合并流  highTempStream，lowTempStream，highAndLowTempStream 三条流
         */
        DataStream<SensorReading> unionStream = highTempStream.union(lowTempStream, highAndLowTempStream);
        unionStream.print("union合流");
        env.execute();

    }
}
