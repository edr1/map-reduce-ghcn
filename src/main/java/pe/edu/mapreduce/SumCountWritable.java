package pe.edu.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SumCountWritable implements Writable {
    private double sum;
    private long count;

    public SumCountWritable() {}

    public SumCountWritable(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() { return sum; }
    public long getCount() { return count; }

    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
    }
}
