package analysis.ResolvedRate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ResolvedRate {

    private Job job;

    public ResolvedRate(Configuration conf, String input, String output) throws IOException {
        this.job = Job.getInstance(conf);

        job.setJobName("Incidents Resolved Rate");
        job.setMapperClass(ResolvedRateMapper.class);
        job.setReducerClass(ResolvedRateReducer.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(PdDistrictPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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