package analysis.ResolvedRate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PdDistrictPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        //when number of reduce tasks equals ten, assign partitioners according to the sequence of PDdistricts
        if (numPartitions == 10) {
            String partitionKey = key.toString();
            int reducetask_num = 0;
            if (partitionKey.contains("BAYVIEW"))
                reducetask_num = 0;
            else if (partitionKey.contains("CENTRAL"))
                reducetask_num = 1;
            else if (partitionKey.contains("INGLESIDE"))
                reducetask_num = 2;
            else if (partitionKey.contains("MISSION"))
                reducetask_num = 3;
            else if (partitionKey.contains("NORTHERN"))
                reducetask_num = 4;
            else if (partitionKey.contains("PARK"))
                reducetask_num = 5;
            else if (partitionKey.contains("RICHMOND"))
                reducetask_num = 6;
            else if (partitionKey.contains("SOUTHERN"))
                reducetask_num = 7;
            else if (partitionKey.contains("TARAVAL"))
                reducetask_num = 8;
            else if (partitionKey.contains("TENDERLOIN"))
                reducetask_num = 9;
            return reducetask_num;
        } else {
            System.err.println("WordCountParitioner can not handle 10 paritions");
            return 0;
        }
    }
}
