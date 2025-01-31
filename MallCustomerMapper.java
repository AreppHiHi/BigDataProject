package com.mallcustomer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MallCustomerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text gender = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line
        String[] fields = value.toString().split(",");

        // Skip the header
        if (!fields[0].equals("CustomerID")) {
            try {
                gender.set(fields[1].trim()); // Get gender from the 2nd column
                context.write(gender, one);  // Emit (Gender, 1)
            } catch (Exception e) {
                System.err.println("Error processing line: " + value.toString());
            }
        }
    }
}