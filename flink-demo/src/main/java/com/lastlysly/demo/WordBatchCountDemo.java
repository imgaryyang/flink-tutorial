package com.lastlysly.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author lastlySly
 * @GitHub https://github.com/lastlySly
 * @create 2020-12-14 16:48
 *
 * 批处理word count
 **/
public class WordBatchCountDemo {
    public static void main(String[] args) {

        try {
            // 创建执行环境
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            // 从文件中读取数据
            String inputPath = "F:\\JetBrains\\IntelliJ IDEA\\learningspace\\flink-tutorial\\flink-demo\\src\\main\\resources\\files\\hello.txt";
            System.out.println(inputPath);
            DataSet<String> inputData = env.readTextFile(inputPath);
            // 对数据集进行处理，按空格分词展开，转换成(word,1)二元组进行统计
            DataSet<Tuple2<String,Integer>> resultSet = inputData.flatMap(new MyFlatMapper())
                    // 按照第一个位置的word分组
                    .groupBy(0)
                    // 将第二个位置上的数据求和
                    .sum(1);
            resultSet.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
