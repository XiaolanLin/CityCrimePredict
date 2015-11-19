package analysis.ResolvedRate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ResolvedRateReducer extends Reducer<Text, Text, Text, Text> {
    //compute the incidents resolved rate for each (year, PDdistrict) pair, set the value equals to the rate
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfIncident = 0;
        int sumOfResolved = 0;
        Text resolvedRate = new Text();
        for (Text val : values) {
            String value = val.toString();
            if (!value.contains("NONE")) {
                sumOfResolved++;
            }
            sumOfIncident++;
        }
        float rate = (float) sumOfResolved / (float) sumOfIncident;
        resolvedRate.set(Float.toString(rate));
        context.write(key, resolvedRate);
    }
}
