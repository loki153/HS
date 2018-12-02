package day1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
//hadoop使用序列化的存储对象，相应的数据格式需要序列化
public class WCReducer extends Reducer <Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text k2,Iterable<LongWritable> v2s,Context context) throws IOException,InterruptedException {
        //接收数据
//        Text k3 = k2;
        //定义一个计数器
        long counter = 0;
        //循环V2s
        for (LongWritable i : v2s){
//            counter += i; i的数据类型不支持这样操作，使用其方法获得值
            counter += i.get();
        }
        //输出
//        context.write(k2,counter);//类型不符合
        context.write(k2,new LongWritable(counter));
    }

}
