package com.demo.task;

import com.demo.agg.CountAgg;
import com.demo.domain.LogEntity;
import com.demo.domain.TopProductEntity;
import com.demo.map.TopProductMapFunction;
import com.demo.sink.TopNRedisSink;
import com.demo.top.TopNHotItems;
import com.demo.util.Property;
import com.demo.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * 热门商品 -> redis
 *
 * @author XINZE
 */
public class TopProductTask {

    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
				.setHost(Property.getStrValue("redis.host"))
//				.setPort(Property.getIntValue("redis.port"))
//				.setDatabase(Property.getIntValue("redis.db"))
				.build();

        Properties properties = Property.getKafkaProperties("topProuct");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));

        DataStream<TopProductEntity> topProduct = dataStream.map(new TopProductMapFunction()).
                // 抽取时间戳做watermark 以 秒 为单位
                assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEntity>() {
                    @Override
                    public long extractAscendingTimestamp(LogEntity logEntity) {
                        return logEntity.getTime() * 1000;
                    }
                })
                // 按照productId 按滑动窗口
                .keyBy("productId").timeWindow(Time.seconds(60),Time.seconds(5))
//                统计传入数据的总数 并封装为topProduceEntity  ，要windowsend干嘛
                .aggregate(new CountAgg(), new WindowResultFunction())
//                同一时间窗口的分到一起
                .keyBy("windowEnd")
//                如果到达windowEnd，那么触发timer计时器，进行排序，并输出为arrayList
//                flatmap就是用于处理arrayList，它将每个arrayList读取后，为每个string生成一个TopProductEntity，并写入排名。
//                发往下游的数据是一个windowEnd所对应的arrayList，不是累积状态。
                .process(new TopNHotItems(topSize))
                .flatMap(new FlatMapFunction<List<TopProductEntity>, TopProductEntity>() {
                    @Override
                    public void flatMap(List<TopProductEntity> TopProductEntitys, Collector<TopProductEntity> collector) throws Exception {
                        System.out.println("-------------Top N Product------------");
                        for (int i = 0; i < TopProductEntitys.size(); i++) {
                            TopProductEntity top = TopProductEntitys.get(i);
                            // 输出排名结果
                            System.out.println(top);
                            collector.collect(top);
                        }
                    }
                });
        ElasticsearchSinkFunction<TopProductEntity> elasticsearchSinkFunction=new ElasticsearchSinkFunction<TopProductEntity>() {
            @Override
            public void process(TopProductEntity topProductEntity, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String,String> data=new HashMap<>();
                data.put("productid",Integer.valueOf(topProductEntity.getProductId()).toString());
                data.put("times",Integer.valueOf(topProductEntity.getActionTimes()).toString());
                data.put("windowEnd",Long.valueOf(topProductEntity.getWindowEnd()).toString());
                IndexRequest request= Requests.indexRequest().index("topproduct").source(data);
                System.out.println(data);
                requestIndexer.add(request);
            }
        };

        ArrayList<HttpHost> httpHosts=new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.244.128",9201,"http"));
        ElasticsearchSink.Builder<TopProductEntity> builder = new ElasticsearchSink.Builder<TopProductEntity>(httpHosts,elasticsearchSinkFunction );
        builder.setBulkFlushMaxActions(1);
        topProduct.addSink(builder.build());
        env.execute("Top N ");
    }
}

/*
1,112,1563799428,1
1,112,1563799428,1
1,113,1563799429,2
1,114,1563799432,3
1,114,1563799433,3
1,114,1563799435,3
1,115,1563799439,3

1,122,1563899999,2
1,129,1571111111,3
1,129,1571113111,1
1,129,1571115222,2
1,129,1571116222,2
1,129,1571117222,2
1,129,1571118222,2
1,129,1571119222,2
1,129,1571120022,2
1,128,1571121022,2
1,128,1571122022,2
1,128,1571123022,2
1,128,1571124022,2
1,128,1571125022,2
*/

