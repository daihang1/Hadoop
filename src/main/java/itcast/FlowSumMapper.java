package itcast;

import com.sun.tools.javac.comp.Flow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//修改
public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    FlowBean v=new FlowBean();
    Text k=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNum=split[1];
        Long upFlow=Long.parseLong(split[split.length-3]);
        Long downFlow=Long.parseLong(split[split.length-2]);
        k.set(phoneNum);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        context.write(k,v);
    }
}
