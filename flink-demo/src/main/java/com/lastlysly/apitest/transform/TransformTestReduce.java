package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-21 17:13
 * reduce聚合 使用
 * KeyedStream（继承自DataStream） 的 api,需要keyby后调用
 **/
public class TransformTestReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 数据源
         */
        DataStream<SensorReading> inputStream = env.addSource(new TransformTestRollingAggregation.MyCustomSourceSingleId());
        /**
         * map转换
         */
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                sensorReading.setId("testId1");
                return sensorReading;
            }
        });

        /**
         * 分组
         */
        KeyedStream<SensorReading,String> keyedStream = dataStream.keyBy(SensorReading::getId);

        /**
         * reduce聚合，取最大温度值，以及当前最新的时间戳
         * (原先max只能对一个字段进行，但实际需要 用 最大的温度值和最新的时间戳组成的对象数据。 因为 max不能对多个字段进行。故采用reduce)
         */
        DataStream<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            /**
             * 两个参数，  第一个参数为 之前聚合的结果（状态），第二个参数为 当前流入的最新数据
             * 返回 聚合后的状态
             * @param sensorReading
             * @param t1
             * @return
             * @throws Exception
             */
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {

                // 取最新的时间戳，取最大的温度值
                return new SensorReading(sensorReading.getId(),
                        t1.getTimestamp(),Math.max(sensorReading.getTemperature(),t1.getTemperature()));
            }
        });
        // lambdas写法
//        keyedStream.reduce((sensorReading1,sensorReading2) -> {
//            // 取最新的时间戳，取最大的温度值
//            return new SensorReading(sensorReading1.getId(),
//                    sensorReading2.getTimestamp(),Math.max(sensorReading1.getTemperature(),sensorReading2.getTemperature()));
//        });
        resultStream.print("reduce使用");
        env.execute();
    }
}
