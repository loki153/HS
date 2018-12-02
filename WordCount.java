package day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //构建一个job对象
//        Job job = new Job(new Configuration(null));
        Job job = Job.getInstance(new Configuration());

        //注意
        job.setJarByClass(WordCount.class);//将main方法所在的类加入

        //设置Mapper相关属性
        job.setMapperClass(WCMapper.class);//需要mapper类
        job.setMapOutputKeyClass(Text.class);//mapper输出的key的类
        job.setMapOutputValueClass(LongWritable.class);//value的类
        FileInputFormat.setInputPaths(job,new Path("/words.txt"));

        //设置Reducer相关属性
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job,new Path("/wcout1130"));
        //提交作业
        job.waitForCompletion(true);//提交等待执行完成
    }
}
