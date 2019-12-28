package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//mapredu共同好友代码第一个map
public class CommonFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        //把文件按照：分割成含有两个字符串的数组
        String[] split = line.split(":");
        //第一个字符串只有一个字符，当成用户
        String user=split[0];

        //第二个字符串按照，分割，当成朋友数组
        String[] friends= split[1].split(",");
        for (String friend : friends) {
            context.write(new Text(friend),new Text(user));
        }
    }
}
