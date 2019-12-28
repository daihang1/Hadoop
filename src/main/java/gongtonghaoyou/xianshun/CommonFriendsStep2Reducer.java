package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//mapreduce共同好友问题第二个reduce
//输出共同好友
public class CommonFriendsStep2Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer=new StringBuffer();
        for (Text value : values) {
            stringBuffer.append(value).append(",");
        }
        context.write(key,new Text(stringBuffer.toString()));
    }
}
