package com.lastlysly.apitest.window;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.beans.VehicleOperationInfo;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2021-01-12 12:27
 * 事件时间 开窗
 **/
public class WindowTestEventTimeWindow {
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> inputStream = env.socketTextStream("localhost",8082);
        // testId1,2021-01-12 12:35:00,2021-01-12 13:35:00,88.0
        DataStream<VehicleOperationInfo> mapStream = inputStream.map(new MapFunction<String, VehicleOperationInfo>() {
            @Override
            public VehicleOperationInfo map(String line) throws Exception {
                String[] fields = line.split(",");
                LocalDateTime getOnTime = LocalDateTime.parse(fields[1],dateTimeFormatter);
                LocalDateTime getOffTime = LocalDateTime.parse(fields[2],dateTimeFormatter);
                return new VehicleOperationInfo(fields[0],getOnTime,getOffTime,Double.parseDouble(fields[3]));
            }
        });
        /**
         * // 升序数据设置事件时间和watermark
         */
//        DataStream<VehicleOperationInfo> dataStream = mapStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<VehicleOperationInfo>() {
//            @Override
//            public long extractAscendingTimestamp(VehicleOperationInfo element) {
//                ZoneId zoneId = ZoneId.systemDefault();
//                Instant instant = element.getGetOnTime().atZone(zoneId).toInstant();
//                return instant.toEpochMilli();
//            }
//        });
        /**
         * 乱序数据设置时间戳和watermark
         */
//        DataStream<VehicleOperationInfo> dataStream = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<VehicleOperationInfo>(Time.seconds(2)) {
//            @Override
//            public long extractTimestamp(VehicleOperationInfo element) {
//                ZoneId zoneId = ZoneId.systemDefault();
//                Instant instant = element.getGetOnTime().atZone(zoneId).toInstant();
//                return instant.toEpochMilli();
//            }
//        });

        /**
         * 自定义
         * forBoundedOutOfOrderness 有界无序
         */
        DataStream<VehicleOperationInfo> dataStream = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // 水位线调慢时间，最大的乱序程度，watermark的延迟时间
                .<VehicleOperationInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                // 如果数据源中的某一个分区/分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
                // 为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                .withIdleness(Duration.ofMinutes(1))
                // 指定EventTime对应的时间字段
                .withTimestampAssigner(new SerializableTimestampAssigner<VehicleOperationInfo>(){
                    @Override
                    public long extractTimestamp(VehicleOperationInfo vehicleOperationInfo, long previousElementTimestamp) {
                        ZoneId zoneId = ZoneId.systemDefault();
                        Instant instant = vehicleOperationInfo.getGetOnTime().atZone(zoneId).toInstant();
                        return instant.toEpochMilli(); // 指定EventTime对应的时间字段
                    }
                }));

//        dataStream.print();
        OutputTag<VehicleOperationInfo> outputTag = new OutputTag<VehicleOperationInfo>("operLate"){};
        // 基于事件时间（上车时间）的开窗聚合，统计15秒内车辆营运金额总和
        SingleOutputStreamOperator<VehicleOperationInfo> incomeSum = dataStream.keyBy(VehicleOperationInfo::getId)
                .timeWindow(Time.seconds(15))
                // 也是基于 事件时间语义，最大的乱序程度，watermark的延迟时间
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("income");

        incomeSum.print();
        incomeSum.getSideOutput(outputTag).print("operLate");

        env.execute();
    }

    public static class MyAssigner implements WatermarkStrategy<VehicleOperationInfo> {

        @Override
        public WatermarkGenerator<VehicleOperationInfo> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return null;
        }

        @Override
        public TimestampAssigner<VehicleOperationInfo> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return null;
        }

        @Override
        public WatermarkStrategy<VehicleOperationInfo> withTimestampAssigner(TimestampAssignerSupplier<VehicleOperationInfo> timestampAssigner) {
            return null;
        }

        @Override
        public WatermarkStrategy<VehicleOperationInfo> withTimestampAssigner(SerializableTimestampAssigner<VehicleOperationInfo> timestampAssigner) {
            return null;
        }

        @Override
        public WatermarkStrategy<VehicleOperationInfo> withIdleness(Duration idleTimeout) {
            return null;
        }
    }
}
