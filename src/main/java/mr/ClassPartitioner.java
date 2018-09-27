package mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wangchao on 2018/9/26.
 */
public class ClassPartitioner extends Partitioner<ScoreBean , NullWritable> {
    public int getPartition(ScoreBean scoreBean, NullWritable nullWritable, int i) {
        return (scoreBean.getGroup().hashCode() & Integer.MAX_VALUE) % i;
    }
}
