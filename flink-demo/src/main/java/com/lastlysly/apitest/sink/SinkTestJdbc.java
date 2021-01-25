package com.lastlysly.apitest.sink;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-31 08:51
 **/
public class SinkTestJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());

        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                sensorReading.setId(sensorReading.getId() + "_jdbc");
                return sensorReading;
            }
        });
        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    /**
     * 实现自定义SinkFunction
     */
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;
        @Override
        public void open(Configuration parameters) throws Exception {
//            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.56.18:3306/mytestdb?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                    "root","123456");
            insertStatement = connection.prepareStatement("insert into sensor_reading (id,get_temp_time, temperature) values (?,?, ?) ");
            updateStatement = connection.prepareStatement("update sensor_reading set temperature = ? where id = ?");

            super.open(parameters);
        }
        // 每来一条数据，调用连接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接执行更新语句，如果没有更新那么就插入
            updateStatement.setDouble(1,value.getTemperature());
            updateStatement.setString(2,value.getId());
            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1,value.getId());
                insertStatement.setLong(2,value.getTimestamp());
                insertStatement.setDouble(3,value.getTemperature());
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
