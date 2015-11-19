package ResolvedRateChange;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;

public class ResolvedRateMapper extends Mapper<Object, Text, Text, Text>{
	//generate the intermediate data with key(year, PDdistrict) and value(Resolution)	
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
