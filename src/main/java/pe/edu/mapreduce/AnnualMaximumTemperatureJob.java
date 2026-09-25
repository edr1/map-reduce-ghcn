package pe.edu.mapreduce;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Pregunta 3: mayor TMAX por año, con estación y fecha. */
public final class AnnualMaximumTemperatureJob {
    private AnnualMaximumTemperatureJob() {}

    public static class MaximumMapper
            extends Mapper<LongWritable, Text, Text, TemperatureRecordWritable> {
        private final Text outputKey = new Text();
        private final TemperatureRecordWritable outputValue = new TemperatureRecordWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (GHCNDRecord.isHeader(line)) {
                context.getCounter(GHCNDCounters.HEADER).increment(1);
                return;
            }
            try {
                GHCNDRecord record = GHCNDRecord.parse(line);
                GHCNDRecord.Measurement tmax = record.tmax();
                if (!tmax.isValid()) {
                    context.getCounter(tmax.isRejectedByQuality()
                            ? GHCNDCounters.QUALITY_REJECTED_TMAX
                            : GHCNDCounters.MISSING_TMAX).increment(1);
                    return;
                }
                outputKey.set(Integer.toString(record.date().getYear()));
                outputValue.set(tmax.value(), record.station(), record.date().toString());
                context.write(outputKey, outputValue);
                context.getCounter(GHCNDCounters.VALID_TMAX).increment(1);
            } catch (IOException e) {
                context.getCounter(GHCNDCounters.MALFORMED_RECORD).increment(1);
            }
        }
    }

    public static class MaximumCombiner extends Reducer<Text, TemperatureRecordWritable,
            Text, TemperatureRecordWritable> {
        private final TemperatureRecordWritable result = new TemperatureRecordWritable();

        @Override
        protected void reduce(Text key, Iterable<TemperatureRecordWritable> values, Context context)
                throws IOException, InterruptedException {
            TemperatureRecordWritable max = maximum(values);
            result.set(max.getTemperature(), max.getStation(), max.getDate());
            context.write(key, result);
        }
    }

    public static class MaximumReducer
            extends Reducer<Text, TemperatureRecordWritable, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<TemperatureRecordWritable> values, Context context)
                throws IOException, InterruptedException {
            TemperatureRecordWritable max = maximum(values);
            result.set(String.format(Locale.US, "%.1f\t%s\t%s",
                    max.getTemperature(), max.getStation(), max.getDate()));
            context.write(key, result);
        }
    }

    private static TemperatureRecordWritable maximum(Iterable<TemperatureRecordWritable> values) {
        double maxTemperature = -Double.MAX_VALUE;
        String station = "";
        String date = "";
        for (TemperatureRecordWritable value : values) {
            if (value.getTemperature() > maxTemperature) {
                maxTemperature = value.getTemperature();
                station = value.getStation();
                date = value.getDate();
            }
        }
        return new TemperatureRecordWritable(maxTemperature, station, date);
    }

    public static Job configure(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "GHCND - máxima temperatura anual");
        job.setJarByClass(AnnualMaximumTemperatureJob.class);
        job.setMapperClass(MaximumMapper.class);
        job.setCombinerClass(MaximumCombiner.class);
        job.setReducerClass(MaximumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TemperatureRecordWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
