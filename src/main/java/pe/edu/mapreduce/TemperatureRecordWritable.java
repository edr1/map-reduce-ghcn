package pe.edu.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TemperatureRecordWritable implements Writable {
    private double temperature;
    private String station = "";
    private String date = "";

    public TemperatureRecordWritable() {}

    public TemperatureRecordWritable(double temperature, String station, String date) {
        set(temperature, station, date);
    }

    public void set(double temperature, String station, String date) {
        this.temperature = temperature;
        this.station = station;
        this.date = date;
    }

    public double getTemperature() { return temperature; }
    public String getStation() { return station; }
    public String getDate() { return date; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(temperature);
        out.writeUTF(station);
        out.writeUTF(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temperature = in.readDouble();
        station = in.readUTF();
        date = in.readUTF();
    }
}
