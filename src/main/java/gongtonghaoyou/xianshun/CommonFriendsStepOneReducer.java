package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//mapredu共同好友代码第一个map
//输出到第二mapre
public class CommonFriendsStepOneReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer stringBuffer=new StringBuffer();
        for (Text value : values) {
            stringBuffer.append(value).append(",");
        }
        context.write(key,new Text(stringBuffer.toString()));
    }
}
