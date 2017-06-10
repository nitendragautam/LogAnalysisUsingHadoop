package com.nitendragautam.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Takes the Client IP address and counts its occurances
 * takes the intermediate key/value pairs generated by the mapper
 * class and sums up the occurence of Client IP address
 */
public class LogAnalyticsReducer extends
        Reducer<Text,IntWritable,Text ,IntWritable>{

    private IntWritable result = new IntWritable();

    public void reduce(Text key ,Iterable<IntWritable> values
            ,Context context) throws IOException , InterruptedException{
int sumOfIPAddressOccurance = 0;

//Iterating every IP address

        for (IntWritable val : values) {
//Counts the add the occurence of every Ip Address
            sumOfIPAddressOccurance += val.get();
        }
        result.set(sumOfIPAddressOccurance);
        context.write(key ,result);
    }


}