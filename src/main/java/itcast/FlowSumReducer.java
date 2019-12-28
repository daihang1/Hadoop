package itcast;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean flow=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow=0;
        long downFlow=0;
        for (FlowBean value : values) {
            upFlow+=value.getUpFlow();
            downFlow+=value.getDownFlow();
        }
        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        context.write(key,flow);
    }
}
