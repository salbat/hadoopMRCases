package com.averageCombiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumPair implements WritableComparable<NumPair> {

    private DoubleWritable first;
    private IntWritable second;

    private void set(DoubleWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public NumPair() {
        set (new DoubleWritable(), new IntWritable());
    }

    public NumPair(DoubleWritable first, IntWritable second) {
        set(first, second);
    }

    public NumPair(double first, int second) {
        set(new DoubleWritable(first), new IntWritable(second));
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public int compareTo(NumPair o) {
         int cmp = first.compareTo(o.first);
         //They are DIFFERENT (NOT ZERO)
         if(cmp != 0) {
            return cmp;
         }

         return second.compareTo(o.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof NumPair) {
            NumPair n = (NumPair) obj;
            return first.equals(n.first) && second.equals(n.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
