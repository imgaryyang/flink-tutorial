package com.lastlysly.apitest.window;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.HashMap;
import java.util.Map;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2021-01-05 12:43
 * 时间窗口
 **/
public class WindowTestTimeWindow {
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
         *         开窗测试
         * 窗口使用前要先keyby，使用完要用 窗口函数
         */
        // 1. 增量聚合函数  min， max， reduce，aggregate 等  ReduceFunction,AggregateFunction
        DataStream<String> resultStream = dataStream.keyBy(SensorReading::getId)
//                .countWindow(10);  // 滚动计数窗口
//                .countWindow(10, 2);  // 滑动计数窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1))); // session窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15))) // 滚动时间窗口 = timeWindow(Time.seconds(15))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10))) // 滑动时间窗口 = timeWindow(Time.seconds(15)，Time.seconds(15))
                .timeWindow(Time.seconds(15))
                /**
                 * 三个泛型参数 1，输入的类型，2，累加器类型（即保存的状态） 3，输出的状态
                 * 案例，按id进行keyby，求各个Id的温度个数，每15秒一个滚动窗口，即每15秒1次
                 */
                .aggregate(new AggregateFunction<SensorReading, Tuple2<String,Integer>, String>() {
                    /**
                     * 创建累加器
                     * @return
                     */
                    @Override
                    public Tuple2<String,Integer> createAccumulator() {
                        Tuple2<String,Integer> accTuple = new Tuple2<String,Integer>();
                        return accTuple;
                    }

                    /**
                     * 数据处理
                     * @param sensorReading
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<String,Integer> add(SensorReading sensorReading, Tuple2<String,Integer> accumulator) {
                        // 求温度值个数 来一条温度数就加+1
                        int count = accumulator.f1 == null ? 1 : accumulator.f1 + 1;
                        return new Tuple2<String,Integer>(sensorReading.getId(),count);
                    }

                    /**
                     * 获得结果
                     * @param
                     * @return
                     */
                    @Override
                    public String getResult(Tuple2<String,Integer> accumulator) {
                        // 输出结果，直接输出累加器的计数
                        return accumulator.f0 + "=====" + accumulator.f1;
                    }

                    /**
                     * 合并，合并两个状态（两个分区）
                     * @param o
                     * @param acc1
                     * @return
                     */
                    @Override
                    public Tuple2<String,Integer> merge(Tuple2<String,Integer> o, Tuple2<String,Integer> acc1) {
                        return new Tuple2<String,Integer>(o.f0,o.f1 + acc1.f1);
                    }
                });

        resultStream.print();



        // 2.全窗口函数，攒齐窗口范围内的数据一次性计算。 ProcessWindowFunction,WindowFunction
        // 案例，按id进行keyby，求各个Id的温度个数，每15秒一个滚动窗口，即每15秒1次
        DataStream<Tuple4<String,Long,Long,Integer>> resultStream2 = dataStream.keyBy(SensorReading::getId).timeWindow(Time.seconds(15))
                /**
                 * ProcessWindowFunction 里 Context包含 Window，上下文，能得到的信息更多
                 */
//                .process(new ProcessWindowFunction<SensorReading, Object, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Object> out) throws Exception {
//
//                    }
//                })
                /**
                 *     输入，输出，key，窗口信息
                 *    WindowFunction<IN, OUT, KEY, W extends Window>
                 *       输出 Tuple4<String,Long,Integer>  温度id(即keyby的key)，窗口开始时间，窗口结束时间，温度数量
                 */
                .apply(new WindowFunction<SensorReading, Tuple4<String,Long,Long,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple4<String,Long,Long,Integer>> out) throws Exception {
                        Integer count = IteratorUtils.toList(input.iterator()).size();

                        out.collect(new Tuple4<String,Long,Long,Integer>(key,window.getStart(),window.getEnd(),count));
                    }
                });
        resultStream2.print("resultStream2=====");




        // 3. 其它可选API， 案例： 对温度值求和。
//          • .trigger() —— 触发器，定义 window 什么时候关闭，触发计算并输出结果
//          • .evictor() —— 移除器，定义移除某些数据的逻辑
//          • .allowedLateness() —— 允许处理迟到的数据
//          • .sideOutputLateData() —— 将迟到的数据放入侧输出流
//          • .getSideOutput() —— 获取侧输出流

        /**
         * late ，侧输出流的标记
         */
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(15))
//                .trigger()  // 触发器，定义window什么时候关闭 和 什么时候计算并输出结果。例如：如果要处理迟到数据，可以选择到点了只计算输出结果不关闭窗口，一般不需要自定义，flink底层基本都定义了
//                .evictor()  // 移除器，定义移除某些数据的逻辑
                .allowedLateness(Time.minutes(1))  // 延迟时间。使用这个以后，15秒的窗口到了只会计算输出，
                // 不会关闭，会再等 1 分钟 收集要进入这个窗口的迟到数据更新计算。
                // 等延时时间结束后，就关闭窗口。但一般只会给一个可以接受的长度，不然窗口一直开着状态一直保存内存很占用性能，但可
                // 以使用 sideOutputLateData ，延时一分钟后，再有迟到数据丢进侧输出流去处理
                .sideOutputLateData(outputTag)
                .sum("temperature");

        /**
         * sumStream在窗口时间或延迟时间里，输出的依旧是窗口计算的数据，要想输出侧输出流需要调用SingleOutputStreamOperator的api, getSideOutput。
         *
         * 后续想要想要得到完整的，就只要 对收集到的两个流（输出流与侧输出流）数据进行处理就好，
         * 场景：正常数据在指定窗口范围内进行流数据处理，范围之外如果有设置延迟时间，则延迟时间之内来一条数据处理一条并更新结果，延迟时间结束如果还有，则会由 侧输出流 做处理，后续合并两个输出流结果即可
         *
         *  问题：怎样算迟到数据？ 请查看时间语义概念。 allowedLateness，sideOutputLateData只在时间事件窗口有效，定义时间语义
         */
        sumStream.getSideOutput(outputTag).print("late");


        env.execute();
    }
}
