package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * group(班级)    name    score
 *
 * Created by wangchao on 2018/9/26.
 */
public class GroupTopN {
    static class GroupTopNMapper extends Mapper<LongWritable, Text, ScoreBean, NullWritable>{
        ScoreBean scoreBean = new ScoreBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            scoreBean.setGroup(Integer.parseInt(fields[0]));
            scoreBean.setName(fields[1]);
            scoreBean.setScore(Double.parseDouble(fields[2]));
            context.write(scoreBean,NullWritable.get());
        }
    }

    static class GroupTopNReducer extends Reducer<ScoreBean,NullWritable,ScoreBean,NullWritable>{
        @Override
        protected void reduce(ScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<NullWritable> iterator = values.iterator();
            for (int i = 0; i < 3; i++) {
                context.write(key,iterator.next());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(GroupTopN.class);
        job.setMapperClass(GroupTopNMapper.class);
        job.setReducerClass(GroupTopNReducer.class);

        //也可以使用Job.setOutputKeyComparatorClass来更灵活的指定key的比较规则
        job.setOutputKeyClass(ScoreBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setGroupingComparatorClass(GroupCompactor.class);
        job.setPartitionerClass(ClassPartitioner.class);
        job.setNumReduceTasks(3);
        job.waitForCompletion(true);
    }
}
