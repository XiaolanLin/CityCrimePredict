package analysis.countByMonth;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/27/15.
 */
public class IncidencePerMonthReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}