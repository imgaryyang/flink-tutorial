package com.lastlysly.apitest.window;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2021-01-07 11:02
 * 计数窗口
 **/
public class WindowTestCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSourceStream());
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }
        });

        /**
         * 开计数窗口测试，这边是 滑动计数窗口的案例
         * 案例：每10条进行一次统计，每两条开一个窗口。按id进行keyby，统计平均温度
         *
         * 这里注意 滑动窗口输出频率是根据滑动步长决定的。所以，下面的案例 一条数据可能会被多次使用求平均
         */
        DataStream<Tuple2<String,Double>> resultStream = dataStream.keyBy(SensorReading::getId)
                .countWindow(10,2)
                .aggregate(new MyAvgTemp());

        resultStream.print("resultStream");

        env.execute();
    }
    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple3<String,Double,Integer>,Tuple2<String,Double>>{

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<String,Double,Integer>();
        }

        @Override
        public Tuple3<String, Double, Integer> add(SensorReading sensorReading, Tuple3<String, Double, Integer> stringDoubleIntegerTuple3) {
            Integer count = stringDoubleIntegerTuple3.f2 == null ? 1 : (stringDoubleIntegerTuple3.f2 + 1);
            Double temp = stringDoubleIntegerTuple3.f1 == null
                    ? (0 + sensorReading.getTemperature()) : (stringDoubleIntegerTuple3.f1 + sensorReading.getTemperature());
            return new Tuple3<String, Double, Integer>(sensorReading.getId(),temp,count);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> stringDoubleIntegerTuple3) {
            if (stringDoubleIntegerTuple3.f1 == null || stringDoubleIntegerTuple3.f2 == null || stringDoubleIntegerTuple3.f2 == 0) {
                return new Tuple2<String,Double>(stringDoubleIntegerTuple3.f0,0d);
            }
            Double avgTemp = stringDoubleIntegerTuple3.f1 / stringDoubleIntegerTuple3.f2;
            return new Tuple2<String,Double>(stringDoubleIntegerTuple3.f0,avgTemp);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc0, Tuple3<String, Double, Integer> acc1) {
            return new Tuple3(acc1.f0,acc0.f1 + acc1.f1,acc0.f2 + acc1.f2);
        }
    }
}
