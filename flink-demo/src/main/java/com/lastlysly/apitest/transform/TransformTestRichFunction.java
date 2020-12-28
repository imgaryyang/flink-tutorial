package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-23 17:29
 * 富函数 示例
 **/
public class TransformTestRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());

        // MapFunction函数
//        DataStream<Tuple2<String,Integer>> dataStream = inputStream.map(new MapFunction<SensorReading, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
//                return new Tuple2<String, Integer>(sensorReading.getId(),sensorReading.getId().length());
//            }
//        });

        // MapFunction 的富函数 RichMapFunction
        DataStream<Tuple2<String,Integer>> dataStream = inputStream.map(new RichMapFunction<SensorReading, Tuple2<String,Integer>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化工作，一般是定义状态，或者建立数据库连接
                System.out.println("open=============");
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                // 一般是关闭连接和清空状态的收尾操作
                System.out.println("close============");
                super.close();
            }

            @Override
            public Tuple2<String,Integer> map(SensorReading sensorReading) throws Exception {
                // getRuntimeContext() 运行上下文
                return new Tuple2<String, Integer>(sensorReading.getId(),getRuntimeContext().getAttemptNumber());
            }
        });
        dataStream.print("富函数");

        env.execute();
    }

}
