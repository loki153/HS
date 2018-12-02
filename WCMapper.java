package day1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WCMapper extends Mapper <LongWritable, Text, Text,LongWritable> {


    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
        //接收数据V1
        String line = value.toString();
        //切分数据
        String[] words = line.split(" ");
        for (String w : words){
            //出现一次记一次1
            context.write(new Text(w),new LongWritable(1));//这里需要序列化，这些是相应的序列化类型
        }

    }
}
