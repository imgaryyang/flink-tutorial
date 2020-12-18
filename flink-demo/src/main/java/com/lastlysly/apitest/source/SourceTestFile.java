package com.lastlysly.apitest.source;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-16 16:58
 *
 * 从文件中 读取 数据
 **/
public class SourceTestFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "F:\\JetBrains\\IntelliJ IDEA\\learningspace\\flink-tutorial\\flink-demo\\src\\main\\resources\\files\\sensor.txt";
        // 从文件中读取
        DataStream<String> dataStream =
                env.readTextFile(filePath);

        dataStream.print();
        ;
        env.execute();

    }
}
