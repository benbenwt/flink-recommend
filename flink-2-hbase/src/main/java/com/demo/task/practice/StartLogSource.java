package com.demo.task.practice;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class StartLogSource implements SourceFunction<StartLog> {

    private boolean running=true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        String[] entryList={"notification","icon"};
        Long[] adIdList={1L,2L,3L,4L};
        Random random=new Random();
        while(running){
            ctx.collect(new StartLog(entryList[random.nextInt(entryList.length)],Calendar.getInstance().getTimeInMillis(),adIdList[random.nextInt(adIdList.length)],adIdList[random.nextInt(adIdList.length)],random.nextLong(),random.nextLong()));
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
