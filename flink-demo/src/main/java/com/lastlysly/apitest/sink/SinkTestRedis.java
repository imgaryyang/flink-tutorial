package com.lastlysly.apitest.sink;

import com.lastlysly.apitest.beans.SensorReading;
import com.lastlysly.apitest.source.MyCustomSourceTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-29 17:41
 **/
public class SinkTestRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> inputStream = env.addSource(new MyCustomSourceTest.MyCustomSource());
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                sensorReading.setId(sensorReading.getId() + "_test");
                return sensorReading;
            }
        });

        /**
         * 定义redis 配置
         */
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.56.18")
                .setPort(6379)
                .setDatabase(0)
                .setPassword("123456").build();

        dataStream.addSink(new RedisSink<>(config, new RedisMapper<SensorReading>() {
            // 定义保存在redis的命令，存成Hash表，hset sensor_temp id temperature
            @Override
            public RedisCommandDescription getCommandDescription() {
                /**
                 * RedisCommand: redis命令
                 * additionalKey :  如果是hash表，则要指定 hash表名。getKeyFromData则为hash表内的id，
                 * 其他的非hash表则无需定义，getKeyFromData为redis的key
                 */
//                return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            // 定义redis的key
            @Override
            public String getKeyFromData(SensorReading sensorReading) {
//                return sensorReading.getId();
//                return "listjihe" + sensorReading.getId();
                return "listjihe2";
            }
            /**
             * 定义redis的value
             * @param sensorReading
             * @return
             */
            @Override
            public String getValueFromData(SensorReading sensorReading) {
                return sensorReading.getTemperature().toString();
            }
        }));
        env.execute();
    }
}
