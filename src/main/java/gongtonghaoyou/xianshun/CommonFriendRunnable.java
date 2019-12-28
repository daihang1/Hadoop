package gongtonghaoyou.xianshun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendRunnable {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();

        Job job2=Job.getInstance(conf);

        job2.setJarByClass(CommonFriendRunnable.class);
        job2.setMapperClass(CommonFriendsStep2Mapper.class);
        job2.setReducerClass(CommonFriendsStep2Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job2,new Path("F:\\data\\output.txt\\part-r-00000"));
        FileOutputFormat.setOutputPath(job2,new Path("F:\\data\\output1.txt"));

        Boolean result1=job2.waitForCompletion(true);
        System.exit(result1?0:1);
    }
}
