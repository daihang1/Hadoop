package gongtonghaoyou.xianshun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

//mapreduce共同好友问题第二个map
public class CommonFriendsStep2Mapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //因为第一个mapreduc输出成以制表符分割开的文本
        String[] split = value.toString().split("\t");
        //反过来把上一个mapreduce的用户当成朋友
        String friend=split[0];
        //把朋友当成用户
        String[] user=split[1].split(",");
        //为避免重复先排序
        Arrays.sort(user);
        //遍历用户数组，前一个元素和后一个元素用-连接当成一个用户，两个用户当成一个用户，那这个用户的朋友就是那两个用户的共同好友
        for (int i=0;i<user.length-2;i++){
            for (int j=i+1;j<user.length-1;j++){
                context.write(new Text(user[i]+"-"+user[j]),new Text(friend));
            }
        }
    }
}
