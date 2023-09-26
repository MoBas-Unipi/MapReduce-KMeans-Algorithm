package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class Utils {
    private Path inputPath;
    private Path outputPath;
    private int pointsNumber;
    private int clustersNumber;
    private int distance;
    private int reducersNumber;
    private float threshold;
    private int maxIterations;

    public void setParameters(Configuration conf, String[] args){
        this.inputPath = new Path(args[0]);
        this.outputPath = new Path(args[1]);
        this.pointsNumber = conf.getInt("points_number",100); //n
        this.clustersNumber = conf.getInt("clusters_number", 2); //k
        this.distance = conf.getInt("distance", 2); //d
        this.reducersNumber = conf.getInt("reducers_number", 1);
        this.threshold = conf.getFloat("threshold", 0.0001f);
        this.maxIterations = conf.getInt("max_iterations", 50);
    }

    public List<Centroid> generateInitialCentroidSet(Configuration conf) {
        return null;
    }
}
