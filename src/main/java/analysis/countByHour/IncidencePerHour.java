package analysis.countByHour;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/19/15.
 */
public class IncidencePerHour {

    private Configuration conf;
    private Job job;
    private String intput;
    private String output;

    public IncidencePerHour(String input, String output) throws IOException {
        this.conf = new Configuration();
        this.job = Job.getInstance(conf);
        this.intput = input;
        this.output = output;

        job.setJobName("Count Most Common Time");
        job.setMapperClass(IncidencePerHourMapper.class);
        job.setReducerClass(IncidencePerHourReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setCombinerClass(IncidencePerHourReducer.class);

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
