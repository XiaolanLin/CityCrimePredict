package analysis.countByMonth;

import analysis.Constants;
import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/27/15.
 */
public class IncidencePerMonthMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private final LongWritable one = new LongWritable(1);
    private CSVParser parser;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.parser = new CSVParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] tokens = this.parser.parseLine(value.toString());
        if (!tokens[0].equals("IncidntNum")) {
            String[] dateSplit = tokens[Constants.INDEX_DATE].split("/");
            int year = Integer.parseInt(dateSplit[2]);
            int month = Integer.parseInt(dateSplit[0]);
            int newKey = year * 100 + month;
            context.write(new IntWritable(newKey), one);
            context.write(new IntWritable(year), one);
        }
    }
}
