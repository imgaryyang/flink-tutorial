package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-18 12:26
 * 转换算子 map,flatmap,filter
 **/
public class TransformTestBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());

        /**
         * 1，map ： one to one 输入一条输出一条。  可以转类型（输出与输入类型可以不同）
         * 例子：SensorReading 转（取） temperature 输出
         */
        DataStream<Double> mapStream = inputStream.map(new MapFunction<SensorReading, Double>() {
            @Override
            public Double map(SensorReading sensorReading) throws Exception {
                return sensorReading.getTemperature();
            }
        });


        /**
         * 2, flatmap 打散  输入一条，可以输出多条。 可以转类型（输出与输入类型可以不同）
         * 例子： 收集每个对象字段数据
         */
        DataStream<Object> flatMapStream = inputStream.flatMap(new FlatMapFunction<SensorReading, Object>() {
            @Override
            public void flatMap(SensorReading sensorReading, Collector<Object> collector) throws Exception {
                collector.collect(sensorReading.getId());
                collector.collect(sensorReading.getTemperature());
                collector.collect(sensorReading.getTimestamp());
            }
        });

        /**
         * 3, filter  过滤筛选  不可以转类型（输出与输入类型不可以不同）
         * 例子
         */
        DataStream<SensorReading> filterStream = inputStream.filter(new FilterFunction<SensorReading>() {
            /**
             *
             * @param sensorReading
             * @return  返回 false 为不要该条数据，true为需要的数据
             * @throws Exception
             */
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                if (sensorReading.getTemperature() > 50) {
                    return true;
                }
                return false;
            }
        });


        // 打印输出
        inputStream.print("inputStream");
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();
    }
}
