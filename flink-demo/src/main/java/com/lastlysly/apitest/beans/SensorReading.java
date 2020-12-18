package com.lastlysly.apitest.beans;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-16 16:40
 *
 * 传感器温度读数的数据类型
 **/
public class SensorReading {
    /**
     * id
     */
    private String id;
    /**
     * 时间戳
     */
    private Long timestamp;
    /**
     * 温度值
     */
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
