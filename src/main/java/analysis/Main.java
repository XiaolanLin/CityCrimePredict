package analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import analysis.count.CountAttribute;
import analysis.countByHour.IncidencePerHour;

/**
 * This is the entrance class of statistics analysis programs
 */
public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: jar <input> <output>");
            System.exit(0);
        }
        ToolRunner.run(new Configuration(), new Main(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String input = args[0];
        String output = args[1];

        // MapReduce job

        // Count by Category
        conf.set("Attribute", "Category");
        Job countCategoryJob = new CountAttribute(conf, input, output + "/output_category").getJob();
        countCategoryJob.waitForCompletion(true);

        // Count by PdDistrict
        conf.set("Attribute", "PdDistrict");
        Job countPDDistrictJob = new CountAttribute(conf, input, output + "/output_pd").getJob();
        countPDDistrictJob.waitForCompletion(true);

        // Count by Hour
        Job countHourJob = new IncidencePerHour(conf, input, output + "/output_hour").getJob();
        return countHourJob.waitForCompletion(true) ? 0 : 1;
    }
}