package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] split = line.split(":");
        String user=split[0];
        String[] friends= split[1].split(",");
        for (String friend : friends) {
            context.write(new Text(friend),new Text(user));
        }
    }
}
