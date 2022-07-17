package com.demo.domain;

/**
 * @author XINZE
 */
public class LogEntity {

    private int userId;
    private int productId;
    private Long time;
    private String action;

    public LogEntity() {
    }

    public LogEntity(int userId, int productId, Long time, String action) {
        this.userId = userId;
        this.productId = productId;
        this.time = time;
        this.action = action;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "LogEntity{" +
                "userId=" + userId +
                ", productId=" + productId +
                ", time=" + time +
                ", action='" + action + '\'' +
                '}';
    }
}
