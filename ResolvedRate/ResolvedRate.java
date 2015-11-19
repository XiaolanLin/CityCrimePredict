package ResolvedRateChange;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVReader;

public class ResolvedRate {
	public static class DistrictPartitioner 
		extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		//when number of reduce tasks equals ten, assign partitioners according to the sequence of PDdistricts
		if (numPartitions == 10) {
			String partitionKey = key.toString();
			int reducetask_num = 0;
			if(partitionKey.contains("BAYVIEW"))
				reducetask_num = 0;
			else if(partitionKey.contains("CENTRAL"))
				reducetask_num = 1;
			else if(partitionKey.contains("INGLESIDE"))
				reducetask_num = 2;
			else if(partitionKey.contains("MISSION"))
				reducetask_num = 3;
			else if(partitionKey.contains("NORTHERN"))
				reducetask_num = 4;
			else if(partitionKey.contains("PARK"))
				reducetask_num = 5;
			else if(partitionKey.contains("RICHMOND"))
				reducetask_num = 6;
			else if(partitionKey.contains("SOUTHERN"))
				reducetask_num = 7;
			else if(partitionKey.contains("TARAVAL"))
				reducetask_num = 8;
			else if(partitionKey.contains("TENDERLOIN"))
				reducetask_num = 9;
			return reducetask_num;
		}
		else{
			System.err.println("WordCountParitioner can not handle 10 paritions");
			return 0;
		}
	}
}
	//generate the intermediate data with key(year, PDdistrict) and value(Resolution)
	public static class ResolvedRateMapper
    extends Mapper<Object, Text, Text, Text>{
		private CSVReader reader;
	    public void map(Object key, Text value, Context context
	    ) throws IOException, InterruptedException {
	    	reader = new CSVReader(new StringReader(value.toString()));
	    	Text keyOfRecord = new Text();
	    	Text valueOfRecord = new Text();
	    	String [] nextLine;
	        while ((nextLine = reader.readNext()) != null) {
	        	if(RecordValid(nextLine)){
	        		StringBuilder contentOfKey = new StringBuilder();
	        		String [] date = nextLine[4].split("/");
	        		String year = date[2];	        		
	        		contentOfKey.append(year).append(",").append(nextLine[6].toUpperCase());
	        		keyOfRecord.set(contentOfKey.toString());
	        		valueOfRecord.set(nextLine[7].toUpperCase());
	        		context.write(keyOfRecord, valueOfRecord);
	        	}
	        }
	    }
	    //make sure the attributes we need are valid
	    public boolean RecordValid(String[] record){
	    	if(record[4].isEmpty()||record[6].isEmpty()||record[7].isEmpty()||record[0].equals("IncidntNum")){
	    		return false;
	    	}
	    	return true;
	    }
	}
	//compute the incidents resolved rate for each (year, PDdistrict) pair, set the value equals to the rate
	public static class ResolvedRateReducer
    extends Reducer<Text,Text,Text,Text> { 
		public void reduce(Text key, Iterable<Text> values,
                Context context
		) throws IOException, InterruptedException {
			int sumOfIncident = 0;
			int sumOfResolved = 0;
			Text resolvedRate = new Text();
			for (Text val : values) {
				String value = val.toString();
				if(!value.contains("NONE")){
					sumOfResolved++;
				}				
				sumOfIncident++;
			}
			float rate = (float)sumOfResolved/(float)sumOfIncident;
			resolvedRate.set(Float.toString(rate));
			context.write(key, resolvedRate);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "Incidents Resolved Rate");
	    job.setJarByClass(ResolvedRate.class);
	    job.setMapperClass(ResolvedRateMapper.class); 
	    job.setReducerClass(ResolvedRateReducer.class);
	    job.setNumReduceTasks(10);
	    job.setPartitionerClass(DistrictPartitioner.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
