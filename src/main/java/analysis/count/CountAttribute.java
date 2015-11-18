package analysis.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Author: yummin
 * Date: 15/11/18
 */
public class CountAttribute {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: jar <attr> <input> <output>");
            System.exit(2);
        }
        String attr = otherArgs[0];
        String input = otherArgs[1];
        String output = otherArgs[2];

        // Set attribute we want to count
        conf.set("Attribute", attr);

        // MapReduce job
        Job job = Job.getInstance(conf);
        job.setJobName("AttributeCount");
        job.setJarByClass(CountAttribute.class);
        job.setMapOutputKeyClass(YearAttributePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(PairsOccurrenceMapper.class);
        job.setPartitionerClass(YearPartitioner.class);
        job.setReducerClass(PairsOccurrenceReducer.class);
        job.setOutputKeyClass(YearPartitioner.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
