package com.lastlysly.apitest.beans;

import java.time.LocalDateTime;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2021-01-12 12:31
 **/
public class VehicleOperationInfo {
    private String id;
    private LocalDateTime getOnTime;
    private LocalDateTime getOffTime;
    private Double income;

    public VehicleOperationInfo() {
    }

    public VehicleOperationInfo(String id, LocalDateTime getOnTime, LocalDateTime getOffTime, Double income) {
        this.id = id;
        this.getOnTime = getOnTime;
        this.getOffTime = getOffTime;
        this.income = income;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LocalDateTime getGetOnTime() {
        return getOnTime;
    }

    public void setGetOnTime(LocalDateTime getOnTime) {
        this.getOnTime = getOnTime;
    }

    public LocalDateTime getGetOffTime() {
        return getOffTime;
    }

    public void setGetOffTime(LocalDateTime getOffTime) {
        this.getOffTime = getOffTime;
    }

    public Double getIncome() {
        return income;
    }

    public void setIncome(Double income) {
        this.income = income;
    }

    @Override
    public String toString() {
        return "VehicleOperationInfo{" +
                "id='" + id + '\'' +
                ", getOnTime=" + getOnTime +
                ", getOffTime=" + getOffTime +
                ", income=" + income +
                '}';
    }
}
