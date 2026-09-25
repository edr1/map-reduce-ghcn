package pe.edu.mapreduce;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Pregunta 2: precipitación acumulada por estación y mes. */
public final class MonthlyPrecipitationJob {
    private static final DateTimeFormatter YEAR_MONTH = DateTimeFormatter.ofPattern("yyyy-MM");
    private MonthlyPrecipitationJob() {}

    public static class PrecipitationMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outputKey = new Text();
        private final DoubleWritable outputValue = new DoubleWritable();

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
                GHCNDRecord.Measurement precipitation = record.precipitation();
                if (!precipitation.isValid()) {
                    context.getCounter(precipitation.isRejectedByQuality()
                            ? GHCNDCounters.QUALITY_REJECTED_PRCP
                            : GHCNDCounters.MISSING_PRCP).increment(1);
                    return;
                }
                outputKey.set(record.station() + "|" + record.date().format(YEAR_MONTH));
                outputValue.set(precipitation.value());
                context.write(outputKey, outputValue);
                context.getCounter(GHCNDCounters.VALID_PRCP).increment(1);
            } catch (IOException e) {
                context.getCounter(GHCNDCounters.MALFORMED_RECORD).increment(1);
            }
        }
    }

    public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            result.set(String.format(Locale.US, "%.1f", sum));
            context.write(key, result);
        }
    }

    public static Job configure(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "GHCND - precipitación acumulada por estación y mes");
        job.setJarByClass(MonthlyPrecipitationJob.class);
        job.setMapperClass(PrecipitationMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
