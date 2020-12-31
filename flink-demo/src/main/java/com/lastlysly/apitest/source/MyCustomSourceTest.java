package com.lastlysly.apitest.source;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-17 16:29
 *  从 自定义数据源 读取数据
 **/
public class MyCustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataSource = env.addSource(new MyCustomSource());

        dataSource.print();
        env.execute();
    }

    /**
     * 自定义数据源
     */
    public static class MyCustomSource implements SourceFunction<SensorReading> {

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
            for (int i = 0; i < 5; i++) {
//                random.nextGaussian() 高斯随机数（正态分布）
                sensorTempMap.put("sensor_" + (i+1),60 + random.nextGaussian() * 20);
            }
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
                if (i == 3) {
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

    /**
     * 自定义 流数据源
     */
    public static class MyCustomSourceStream implements SourceFunction<SensorReading> {

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
            while (running) {
                Double newTemp = 60 + random.nextGaussian() * 20 + random.nextGaussian();
//                    调用 collect方法收集生成的数据。
                ctx.collect(new SensorReading("test_" + random.nextInt(20),System.currentTimeMillis(),newTemp));
                // 控制输出频率
                TimeUnit.SECONDS.sleep(1);
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
