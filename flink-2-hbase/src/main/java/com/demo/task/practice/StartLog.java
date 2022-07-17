package com.demo.task.practice;

public class StartLog {
    private String entry;
    private Long ts;
    private Long userId;


    private Long adId;
    private Long adTime;
    private Long skipAdTime;

    public StartLog() {
    }

    public StartLog(String entry,
                    Long ts,
                    Long userId,
                    Long adId,
                    Long adTime,
                    Long skipAdTime){
        this.entry=entry;
        this.ts=ts;
        this.userId=userId;
        this.adId=adId;
        this.adTime=adTime;
        this.skipAdTime=adTime;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getEntry() {
        return entry;
    }

    public void setEntry(String entry) {
        this.entry = entry;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public Long getAdTime() {
        return adTime;
    }

    public void setAdTime(Long adTime) {
        this.adTime = adTime;
    }

    public Long getSkipAdTime() {
        return skipAdTime;
    }

    public void setSkipAdTime(Long skipAdTime) {
        this.skipAdTime = skipAdTime;
    }

    @Override
    public String toString() {
        return "StartLog{" +
                "entry='" + entry + '\'' +
                ", ts=" + ts +
                ", userId=" + userId +
                ", adId=" + adId +
                ", adTime=" + adTime +
                ", skipAdTime=" + skipAdTime +
                '}';
    }
}
