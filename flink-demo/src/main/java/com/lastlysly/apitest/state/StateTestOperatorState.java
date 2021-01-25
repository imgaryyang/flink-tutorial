package com.lastlysly.apitest.state;

import com.lastlysly.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2021-01-20 17:14
 * 算子状态
 **/
public class StateTestOperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost",8082);
        // 转换成SensorReading类型
        // sensor_1,1547718199,35.8
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 算子状态
        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());

        resultStream.print();

        env.execute();
    }
    // 自定义MapFunction,    ListCheckpointed 1.11后弃用
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, CheckpointedFunction {
        // 定义一个本地变量，作为算子状态
        private Integer count = 0;
        // 这种状态通过用户传入的reduceFunction，每次调用add方法添加值的时候，会调用reduceFunction，最后合并到一个单一的状态值。
//        private ReducingState<Integer> countPerKey;

        // 即key上的状态值为一个列表。可以通过add方法往列表中附加值；也可以通过get()方法返回一个Iterable<T>来遍历状态值。
        private ListState<Integer> countPerPartition;
        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
//            countPerKey.add(1);
            return count;
        }

        /**
         * 保存快照
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countPerPartition.clear();
            countPerPartition.add(count);
        }

        /**
         * 恢复
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // get the state data structure for the per-key state
            // getKeyedStateStore 这个是 键控状态，，需要先keyby。
//            countPerKey = context.getKeyedStateStore().getReducingState(
//                    new ReducingStateDescriptor<>("perKeyCount", new ReduceFunction<Integer>() {
//                        @Override
//                        public Integer reduce(Integer t0, Integer t1) throws Exception {
//                            return t0 + t1;
//                        }
//                    }, Integer.class));


            // get the state data structure for the per-key state
            countPerPartition = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("perPartitionCount", Integer.class));

            // initialize the "local count variable" based on the operator state
            for (Integer l : countPerPartition.get()) {
                count += l;
            }
        }
    }
}
