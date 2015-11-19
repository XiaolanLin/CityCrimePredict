package ResolvedRateChange;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResolvedRate {
	private Configuration conf;
	private Job job;
	private String input;
	private String output;
	public ResolvedRate(String input, String output) throws IOException {
		this.conf = new Configuration();
	    this.job = new Job(this.conf, "Incidents Resolved Rate");
	    this.input = input;
	    this.output = output;
	    
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





