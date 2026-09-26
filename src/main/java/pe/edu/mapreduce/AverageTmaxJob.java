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
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

/** Pregunta 1: temperatura máxima promedio por estación y año. */
public final class AverageTmaxJob {
    private AverageTmaxJob() {}

    public static class TmaxMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        private final Text outputKey = new Text();
        private final SumCountWritable outputValue = new SumCountWritable();

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
                outputKey.set(record.station() + "|" + record.date().getYear());
                outputValue.set(tmax.value(), 1);
                context.write(outputKey, outputValue);
                context.getCounter(GHCNDCounters.VALID_TMAX).increment(1);
            } catch (IOException e) {
                context.getCounter(GHCNDCounters.MALFORMED_RECORD).increment(1);
            }
        }
    }

    public static class SumCountCombiner
            extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
        private final SumCountWritable result = new SumCountWritable();

        @Override
        protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (SumCountWritable value : values) {
                sum += value.getSum();
                count += value.getCount();
            }
            result.set(sum, count);
            context.write(key, result);
        }
    }

    public static class AverageReducer
            extends Reducer<Text, SumCountWritable, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (SumCountWritable value : values) {
                sum += value.getSum();
                count += value.getCount();
            }
            result.set(String.format(Locale.US, "%.2f\t%d", sum / count, count));
            context.write(key, result);
        }
    }

    public static Job configure(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "GHCND - promedio TMAX por estación y año");
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setJarByClass(AverageTmaxJob.class);
        job.setMapperClass(TmaxMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
