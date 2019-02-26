package com.zero.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by void on 2019/2/26.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : iterable){
            sum = sum+i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
