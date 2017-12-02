package com.averageCombiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, NumPair> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");
        try {
            String mStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);
            context.write(new Text(mStatus), new NumPair(hrs, 1));
        } catch (Exception e) { }
    }
}
