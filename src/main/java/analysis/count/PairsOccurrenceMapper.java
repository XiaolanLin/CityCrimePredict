package analysis.count;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

import analysis.Constants;

/**
 * Count (Year, Attribute) pairs occurrence using Order Inversion algorithm.
 * <p/>
 * Attribute could be Category, PdDistrict.
 * <pre>
 * {@code
 * Configuration conf = new Configuration();
 * conf.set("Attribute", "PdDistrict");
 * }
 * </pre>
 *
 * @Author: yummin
 * Date: 15/11/18
 */
public class PairsOccurrenceMapper extends Mapper<LongWritable, Text, YearAttributePair, IntWritable> {

    private CSVParser parser;
    private int attrIndex;
    private HashMap<Integer, MutableInt> yearCounter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Create CSV parser, use default quote char and separator
        this.parser = new CSVParser();
        // Find attribute index of CSV
        String s = context.getConfiguration().get("Attribute");
        if (s.equals("PdDistrict"))
            this.attrIndex = Constants.INDEX_PD_DISTRICT;

        else if (s.equals("Category"))
            this.attrIndex = Constants.INDEX_CATEGORY;
        // Initialize counter
        yearCounter = new HashMap<Integer, MutableInt>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parsing CSV row
        String[] tokens = this.parser.parseLine(value.toString());
        if (!tokens[0].equals("IncidntNum")) {
            String date[] = tokens[Constants.INDEX_DATE].split("/");
            String attr = tokens[attrIndex];
            int year = Integer.parseInt(date[2]);
            // Update counter
            MutableInt count = yearCounter.get(year);
            if (count == null)
                yearCounter.put(year, new MutableInt());
            else
                count.increment();
            // Emit
            context.write(new YearAttributePair(year, attr), new IntWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Iterate through counter
        for (Map.Entry<Integer, MutableInt> entry : yearCounter.entrySet()) {
            int year = entry.getKey();
            int value = entry.getValue().get();
            // Emit
            context.write(new YearAttributePair(year, "*"), new IntWritable(value));
        }
        super.cleanup(context);
    }
}

class MutableInt {
    // note that we start at 1 since we're counting
    int value = 1;

    public void increment() {
        ++value;
    }

    public int get() {
        return value;
    }
}