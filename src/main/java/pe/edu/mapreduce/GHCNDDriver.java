package pe.edu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public final class GHCNDDriver {
    private GHCNDDriver() {}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remaining = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remaining.length != 3) {
            System.err.println("Uso: hadoop jar ghcnd-mapreduce-1.0.0.jar "
                    + "<avg-tmax|monthly-prcp|annual-max> <entrada> <salida>");
            System.exit(2);
        }

        Path input = new Path(remaining[1]);
        Path output = new Path(remaining[2]);
        Job job;
        switch (remaining[0]) {
            case "avg-tmax":
                job = AverageTmaxJob.configure(conf, input, output);
                break;
            case "monthly-prcp":
                job = MonthlyPrecipitationJob.configure(conf, input, output);
                break;
            case "annual-max":
                job = AnnualMaximumTemperatureJob.configure(conf, input, output);
                break;
            default:
                throw new IllegalArgumentException("Análisis desconocido: " + remaining[0]);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
