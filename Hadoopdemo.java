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

public class Hadoopdemo {
    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.103:9000"),new Configuration());//获取HDFS位置与配置，新建一个fs对象
        InputStream in = fs.open(new Path("/wc.txt"));//获取下载文件的路径
        OutputStream out = new FileOutputStream("/home/loki/hadooptest/wc.txt");
        IOUtils.copyBytes(in,out,4096,true);
    }
}
