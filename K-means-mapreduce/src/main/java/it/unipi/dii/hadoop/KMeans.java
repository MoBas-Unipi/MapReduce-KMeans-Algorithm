package it.unipi.dii.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;



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


        //centroids set initialization


    }
}
