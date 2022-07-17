package com.demo.task.practice;

public class UVPVEntity {
    private Long userId;
    private Long productId;
    private Long ts;

    public UVPVEntity(Long userId, Long productId, Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.ts = ts;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UVPVEntity{" +
                "userId=" + userId +
                ", productId=" + productId +
                ", ts=" + ts +
                '}';
    }
}
