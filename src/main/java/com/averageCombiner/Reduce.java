package com.averageCombiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, NumPair, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (NumPair v : values) {
            sum += v.getFirst().get();
            count += v.getSecond().get();
        }

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
    }
}
