package analysis.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: yummin
 * Date: 15/11/18
 */
public class PairsOccurrenceReducer extends Reducer<YearAttributePair, IntWritable, YearAttributePair, IntWritable> {

    private IntWritable totalCount;
    private IntWritable currentYear;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Initialize variables
        totalCount = new IntWritable();
        currentYear = new IntWritable(0);
    }

    @Override
    protected void reduce(YearAttributePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.attribute.toString().equals("*")) {
            if (key.year.equals(currentYear))
                totalCount.set(totalCount.get() + getTotalCount(values));
            else {
                // Reset variables
                currentYear.set(key.year.get());
                totalCount.set(0);
                totalCount.set(getTotalCount(values));
            }
            context.write(key, totalCount);
        } else {
            int count = getTotalCount(values);
            context.write(key, new IntWritable(count));
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values)
            count += value.get();
        return count;
    }
}
