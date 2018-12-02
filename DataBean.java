import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Loki
 * @Date: 2018/12/2 15:13
 * @Project: HS
 * @Description:
 */
public class DataBean implements Writable {
    private String telNo;//电话
    private long upPayload;//上行流量
    private long downPayload;//下行流量
    private long totalPayload;//总流量


    //构造一个有参数的构造方法
    public DataBean(String telNo, long  upPayload, long downPayload){
        this.telNo = telNo;
        this.upPayload = upPayload;
        this.downPayload = downPayload;
        this.totalPayload = upPayload + downPayload;
    }

    //相应地，需要构造一个无参数的构造方法
    public DataBean(){}

    public long getUpPayload() {
        return upPayload;
    }

    public long getDownPayload() {
        return downPayload;
    }

    //serialize 在这里实现序列化
    public void write(DataOutput out) throws IOException {
        //将对象的内容写到流里面
        out.writeUTF(telNo);
        out.writeLong(upPayload);
        out.writeLong(downPayload);
        out.writeLong(totalPayload);
    }
    //deserialize 反序列化
    //注意：1.顺序 2.类型
    public void readFields(DataInput in) throws IOException {
        //将字节流的内容读取给对象赋值
        this.telNo = in.readUTF();
        this.upPayload = in.readLong();
        this.downPayload = in.readLong();
        this.totalPayload = in.readLong();
    }

    @Override
    public String toString(){
        return this.upPayload + "\t" + this.downPayload + "\t" + this.totalPayload;

    }
}
