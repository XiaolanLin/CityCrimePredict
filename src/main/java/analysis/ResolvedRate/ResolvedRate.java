package ResolvedRateChange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResolvedRate {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "Incidents Resolved Rate");
	    job.setJarByClass(ResolvedRate.class);
	    job.setMapperClass(ResolvedRateMapper.class); 
	    job.setReducerClass(ResolvedRateReducer.class);
	    //job.setNumReduceTasks(10);
	    job.setPartitionerClass(PdDistrictPartitioner.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
