package it.unipi.dii.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class Utils {

    public void setParameters(Configuration conf, String[] args){
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        final int pointsNumber = conf.getInt("points_number",100); //n
        final int clustersNumber = conf.getInt("clusters_number", 2); //k
        final int distance = conf.getInt("distance", 2); //d
        final int reducersNumber = conf.getInt("reducers_number", 1);
        final float threshold = conf.getFloat("threshold", 0.0001f);
        final int maxIterations = conf.getInt("max_iterations", 50);

    }
}
