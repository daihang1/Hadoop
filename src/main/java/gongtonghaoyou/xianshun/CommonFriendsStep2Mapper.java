package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;


public class CommonFriendsStep2Mapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String friend=split[0];
        String[] user=split[1].split(",");
        Arrays.sort(user);
        for (int i=0;i<user.length-2;i++){
            for (int j=i+1;j<user.length-1;j++){
                context.write(new Text(user[i]+"-"+user[j]),new Text(friend));
            }
        }
    }
}
