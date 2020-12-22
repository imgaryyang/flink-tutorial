package com.lastlysly.apitest.transform;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-21 12:29
 * 滚动聚合算子 （max，maxby）
 **/
public class TransformTestRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceSingleId());

        /**
         * 转换成 SensorReading 类型 （因为 自定义数据源 本就是 SensorReading，这里为演示map的api功能，在id后补充 0 代表处理）
         */
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
//            @Override
//            public SensorReading map(SensorReading sensorReading) throws Exception {
//                sensorReading.setId(sensorReading.getId() + "buchong");
//                return sensorReading;
//            }
//        });
        // 使用 lambdas 表达式
        DataStream<SensorReading> dataStream = inputStream.map(sensorReading -> {
            sensorReading.setId(sensorReading.getId() + "buchong");
            return sensorReading;
        });

        /**
         * 分组
         */

        /**
         * 滚动聚合，取当前最大的温度值
         */

        /**
         * 0, keyby  使用
         */
        // 写死字段
//        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // 使用KeySelector
//        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {
//            @Override
//            public String getKey(SensorReading sensorReading) throws Exception {
//                return sensorReading.getId();
//            }
//        });
        // 使用KeySelector lambdas 表达式
//        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(data -> data.getId());
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);


        /**
         * 1，max ：  使用 滚动聚合，  取当前最大的温度值 。 输出结果出现 只更新了 被 滚动聚合的字段，其他字段一直不变（不是对应原来对象的其他的字段值）
         *      如： user{id: 1, age:15, key: '123'}, user{id: 2, age:16, key: '123'}, user{id: 3, age:17, key: '123'}
         *      按 age 做 max  , 输出会出现 user{id: 1, age:17, key: '123'} 。只更新了 age，其他不更新（使用第一条输出，不确定是哪个）
         * 2, maxBy ： 输出结果全更新（原来对象的字段数据。不会出现如上max的情况）
         */
        DataStream<SensorReading> resultStreamMax = keyedStream.max("temperature");
        DataStream<SensorReading> resultStreamMaxBy = keyedStream.maxBy("temperature");
        resultStreamMax.print("max 使用");
        resultStreamMaxBy.print("maxBy 使用");
        env.execute();
    }


    /**
     * 自定义数据源 (id相同的数据，用于测试 max，maxby 聚合等)
     */
    public static class MyCustomSourceSingleId implements SourceFunction<SensorReading> {

        // 定义标志位，用于控制数据的产生
        private boolean running = true;

        /**
         * 数据源执行  生成数据
         * @param ctx  数据源Source任务的 上下文
         * @throws Exception
         */
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置10个传感器的
            HashMap<String,Double> sensorTempMap = new HashMap<>(16);
//                random.nextGaussian() 高斯随机数（正态分布）
            sensorTempMap.put("sensor_1",60 + random.nextGaussian() * 20);
            int i = 0;
            while (running) {
                for(String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
//                    调用 collect方法收集生成的数据。
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));

                }
                i ++;
                // 控制输出频率
                TimeUnit.SECONDS.sleep(1);
                if (i == 10) {
                    // 控制取消
                    cancel();
                }


            }
        }

        /**
         * 数据生成控制  取消或退出
         */
        @Override
        public void cancel() {
            running = false;
        }
    }

}
