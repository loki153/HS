package day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
//import org.mortbay.jetty.AbstractGenerator;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Hadoopdemo2 {

    FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.103:9000"),new Configuration(),"root");//获取HDFS位置与配置，新建一个fs对象
    }

    @Test
    public void testUpload() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.103:9000"),new Configuration(),"root");
        //读取本地文件系统的文件
        InputStream in = new FileInputStream("/home/loki/test.txt");
        OutputStream out = fs.create(new Path("/test/test.txt"));//目的文件的路径
        IOUtils.copyBytes(in,out,4096,true);
    }


    public static void main(String[] args){

    }
}
