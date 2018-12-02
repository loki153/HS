package day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RPCclient {
    public static void main(String[] args) throws IOException {
        Bizable proxy = RPC.getProxy(Bizable.class,10010,new InetSocketAddress("192.168.1.107",9527),new Configuration());
        String result = proxy.sayHi("Tomcat");
        System.out.println(result);
        RPC.stopProxy(proxy);

    }
}
