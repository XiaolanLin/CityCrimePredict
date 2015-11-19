package analysis.countByHour;

import java.io.IOException;

/**
 * Created by Xiaolan Lin on 11/19/15.
 */
public class TestJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        IncidencePerHour job = new IncidencePerHour(args[0], args[1]);
        job.run();
    }
}
