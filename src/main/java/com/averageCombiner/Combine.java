package com.averageCombiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combine extends Reducer<Text, NumPair, Text, NumPair> {

    @Override
    protected void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for(NumPair value : values) {
            sum += value.getFirst().get();
            count += value.getSecond().get();
        }

        context.write(key, new NumPair(sum, count));
    }
}
