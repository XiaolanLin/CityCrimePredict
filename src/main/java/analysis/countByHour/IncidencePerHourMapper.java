package analysis.countByHour;

import analysis.Constants;
import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/18/15.
 */
public class IncidencePerHourMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private final LongWritable one = new LongWritable(1);
    private CSVParser parser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.parser = new CSVParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = this.parser.parseLine(value.toString());
        if (!tokens[0].equals("IncidntNum")) {
            String hourToString = tokens[Constants.INDEX_TIME].split(":")[0];
            int hour = Integer.parseInt(hourToString);
            context.write(new IntWritable(hour), one);
        }
    }
}
