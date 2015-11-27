package analysis.countByMonth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/27/15.
 */
public class IncidencePerMonth {

    private Job job;

    public IncidencePerMonth(Configuration conf, String input, String output) throws IOException {
        this.job = Job.getInstance(conf);

        job.setJobName("Count Crime By Month");
        job.setMapperClass(IncidencePerMonthMapper.class);
        job.setReducerClass(IncidencePerMonthReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setCombinerClass(IncidencePerMonthReducer.class);

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
