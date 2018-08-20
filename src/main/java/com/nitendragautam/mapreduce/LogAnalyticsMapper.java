package com.nitendragautam.mapreduce;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper Class which parses the Log line and Gives Output as Key/Value Pair for Each Key
 * Input for the Mapper will be the Log Line
 * Here Key will be different IP Address and Value will be their count (1)
 * Output : IP_ADDRESS,1
 *
 *
 */
 
public class LogAnalyticsMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private IntWritable clientAddressCount = new IntWritable(1);


    AccessLogsParser accessLogsParser = new AccessLogsParser();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

          String logRecord = value.toString();

        AccessLogsRecord accessLogRecord =
                accessLogsParser.parseAccessLogs(logRecord);
         //Parsing the Log Record to get the IP Address and Setting count of IP Address as one
        context.write(new Text(accessLogRecord.getClientAddress()), clientAddressCount);

        //Through Context.Write Output from this Mapper will be passed to Reducer as Intermediate Key/Value Pairs
  

    }


}
