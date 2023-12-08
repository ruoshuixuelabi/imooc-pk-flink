package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domain.Access;
import com.imooc.flink.domain.EventCatagoryProductCount;
import com.imooc.flink.udf.TopNAggregateFunction;
import com.imooc.flink.udf.TopNWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TopNAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.json")).build();
        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        // json ==> 自定义对象
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            // TODO... 把这些异常的数据记录到某个地方去
                            return null;
                        }
                    }
                })//
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Access>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.time))//
//                .assignTimestampsAndWatermarks(
//                        new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
//                            @Override
//                            public long extractTimestamp(Access element) {
//                                return element.time;
//                            }
//                        }
//                )//
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                });
        WindowedStream<Access, Tuple3<String, String, String>, TimeWindow> windowStream = cleanStream.keyBy(
                new KeySelector<Access, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Access value) throws Exception {
                        return Tuple3.of(value.event, value.product.category, value.product.name);
                    }
                }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));
        // 作用上WindowFunction
        SingleOutputStreamOperator<EventCatagoryProductCount> aggStream = windowStream.aggregate(new TopNAggregateFunction(), new TopNWindowFunction());
        aggStream.keyBy(new KeySelector<EventCatagoryProductCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(EventCatagoryProductCount value) throws Exception {
                return Tuple4.of(value.event, value.catagory, value.start, value.end);
            }
        }).process(new KeyedProcessFunction<Tuple4<String, String, Long, Long>, EventCatagoryProductCount, List<EventCatagoryProductCount>>() {
            private transient ListState<EventCatagoryProductCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<EventCatagoryProductCount>("cnt-state", EventCatagoryProductCount.class));
            }

            @Override
            public void processElement(EventCatagoryProductCount value, Context ctx, Collector<List<EventCatagoryProductCount>> out) throws Exception {
                listState.add(value);
                // 注册一个定时器
                ctx.timerService().registerEventTimeTimer(value.end + 1);
            }
            /**
             * 在这里完成TopN操作
             * @param timestamp The timestamp of the firing timer.
             * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
             *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
             *     registering timers and querying the time. The context is only valid during the invocation
             *     of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<EventCatagoryProductCount>> out) throws Exception {
                ArrayList<EventCatagoryProductCount> list = Lists.newArrayList(listState.get());
                list.sort((x, y) -> Long.compare(y.count, x.count));
                ArrayList<EventCatagoryProductCount> sorted = new ArrayList<>();
                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    EventCatagoryProductCount bean = list.get(i);
                    sorted.add(bean);
                }
                out.collect(sorted);
            }
        }).print().setParallelism(1);
        env.execute();
    }
}