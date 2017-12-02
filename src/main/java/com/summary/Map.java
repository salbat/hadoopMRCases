package com.summary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");
        try {
            String mStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);
            context.write(new Text(mStatus), new DoubleWritable(hrs));
        } catch (Exception e) { }
    }
}
