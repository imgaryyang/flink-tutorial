package com.lastlysly.apitest.source;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-16 16:40
 *
 * 数据源 ： 从 集合 读取
 **/
public class SourceTestCollection {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);


        // source 从集合读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1",1547718199L,35.8),
                new SensorReading("sensor_3",1547718201L,15.4),
                new SensorReading("sensor_5",1547718202L,6.7),
                new SensorReading("sensor_10",1547718205L,38.1)
        ));

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 4, 67, 189);
        // 打印流，可以传参数 表示当前流名称
        dataStream.print("dataStream");
        integerDataStreamSource.print("integerDataStreamSource");


        // 启动任务，可以传参数，为 jobName
        env.execute();

    }
}
