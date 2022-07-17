package com.demo.task.practice;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class UVPVSource implements SourceFunction<UVPVEntity> {
    private boolean running=true;
    @Override
    public void run(SourceContext<UVPVEntity> ctx) throws Exception {
        Random random=new Random();
        Long[] userIdList={101L,102L,103L,104L,105L};
        Long[] productIdList={1L,2L,3L,4L,5L,6L,7L,8L};
        while(running){
            Thread.sleep(100);
            ctx.collect(new UVPVEntity(userIdList[random.nextInt(userIdList.length)],productIdList[random.nextInt(productIdList.length)], Calendar.getInstance().getTimeInMillis()-17*24*3600*1000));
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
