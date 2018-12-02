package day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class Hadoopdemo3 {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.103:9000"),new Configuration(),"root");//获取HDFS位置与配置，新建一个fs对象
        //使用hadoop自带的拷贝文件方法更加有效
//        fs.copyToLocalFile(new Path("/test/test.txt"),new Path("/home/loki/fromhadoop.txt"));
        //删除文件
//        fs.delete(new Path("/test/test.txt"),false);;
        //创建一个文件夹
        boolean mkdirs = fs.mkdirs(new Path("/cthlu"));
        System.out.println(mkdirs);
        //删除一个文件夹
        boolean flag = fs.delete(new Path("/cthlu"));
        System.out.println(flag);
    }
}
