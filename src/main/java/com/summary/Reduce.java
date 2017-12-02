package com.summary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (DoubleWritable v : values) {
            sum += v.get();
            count++;
        }

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
    }
}
