package com.demo.task.practice;

import com.demo.domain.LogEntity;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<LogEntity> {

    private boolean running=true;
    @Override
    public void run(SourceContext<LogEntity> ctx) throws Exception {
        Random random=new Random();

        int [] userIds={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
        int [] productIds={101,102,103,104,105,106,107,108,109,110,111,112};
        int [] productIds1={101,102,103,104,105,108,109,110,111,112};
        Integer action;

        while (running){
            Thread.sleep(100);
            action=random.nextInt(3);
            ctx.collect(new LogEntity(
                userIds[random.nextInt(userIds.length)],
                    productIds1[random.nextInt(productIds1.length)],
                        Calendar.getInstance().getTimeInMillis()-(17*24)*3600*1000,
                    action.toString()
            ));
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
