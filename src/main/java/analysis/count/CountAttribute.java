package analysis.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: yummin
 * Date: 15/11/18
 */
public class CountAttribute {

    private Job job;

    public CountAttribute(Configuration conf, String input, String output) throws IOException {
        this.job = Job.getInstance(conf);

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
    }

    public Job getJob() {
        return job;
    }

    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
