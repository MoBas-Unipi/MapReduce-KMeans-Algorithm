package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class KMeans {
    private final static Utils utils = new Utils();

    public static void main (String[] args) throws IOException {
        //create configuration object and load config file
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        //check number of arguments
        if(args.length != 2) {
            System.err.println("Error! Usage: <input path> <output path> ");
            System.exit(1);
        }

        //set parameters
        utils.setParameters(conf,args);

        //centroids set generation
        List<Centroid> initialCentroidSet = new ArrayList<Centroid>();
        initialCentroidSet = utils.generateInitialCentroidSet(conf);

        //add centroids set to Hadoop Configuration
        utils.setCentroidsSetInConfiguration(conf, initialCentroidSet);

    }
}
