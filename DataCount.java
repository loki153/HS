import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Loki
 * @Date: 2018/12/2 15:10
 * @Project: HS
 * @Description:
 */
public class DataCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DataCount.class);

        job.setMapperClass(DCMapper.class);
        //在满足K2 V2 和 K3 V3类型一致时可以省略
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));//接收输入路径，放到args里面

        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));//接收输出路径，放到args第二个位置
        job.waitForCompletion(true);



    }
    //需要编写databean
    public static class DCMapper extends Mapper <LongWritable, Text,Text,DataBean>{
        @Override
        protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
            String line = value.toString();//读取一行内容
            String[] fields = line.split("\t");
            String telNo = fields[1];
            //这里可以用try catch包起来
            long up = Long.parseLong(fields[8]);
            long down = Long.parseLong(fields[9]);
            DataBean bean = new DataBean(telNo,up,down);
            //telNo作为K2,其余按照对象保存作为V2
            context.write(new Text(telNo),bean);
        }


    }

    public static class DCReducer extends Reducer<Text,DataBean,Text,DataBean>{
        @Override
        protected void reduce(Text key, Iterable<DataBean> v2s, Context context) throws IOException, InterruptedException {
            long up_sum = 0;
            long down_sum = 0;
            for (DataBean bean:v2s){
                up_sum += bean.getUpPayload();
                down_sum += bean.getDownPayload();
            }
            DataBean bean = new DataBean("",up_sum,down_sum);
            context.write(key,bean);//对象会自动调用tostring方法
        }
    }
}
