package analysis.countByHour;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/18/15.
 */
public class InstanceByHourReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
