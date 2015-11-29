package analysis.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition by year
 *
 * @Author: yummin
 * Date: 15/11/18
 */
public class YearPartitioner extends Partitioner<YearAttributePair, IntWritable> {

    @Override
    public int getPartition(YearAttributePair yearAttributePair, IntWritable intWritable, int numPartitions) {
        return yearAttributePair.year.hashCode() % numPartitions;
    }
}