package com.zero;

import com.zero.hadoop.WordCountMapper;
import com.zero.hadoop.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by void on 2019/2/26.
 */
public class WordCountTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置对象
        Configuration conf = new Configuration();
        //创建job对象
        Job job = Job.getInstance(conf, "word count");
        //设置运行job的类
        job.setJarByClass(WordCountTest.class);
        //设置mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置reducer类
        job.setReducerClass(WordCountReducer.class);

        //设置map输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job
        boolean b = job.waitForCompletion(true);
        if(!b){
            System.out.println("wordCount fail");
        }
    }
}
