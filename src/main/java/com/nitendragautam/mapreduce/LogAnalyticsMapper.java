package com.nitendragautam.mapreduce;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper Class which takes log line as Input and Parses it

 */
public class LogAnalyticsMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private IntWritable clientAddressCount = new IntWritable(1);


    AccessLogsParser accessLogsParser = new AccessLogsParser();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Long tapiStart = System.currentTimeMillis()/1000;
        System.out.println("Hadoop MapReduce Job Start Time in Seconds: " +tapiStart);
        System.out.println("Hadoop MapReduce Job Start Time: " +DateUtility.getTodaysDate());
        String logRecord = value.toString();

        AccessLogRecord accessLogRecord =
                accessLogsParser.parseAccessLogs(logRecord);
         //Parsing the IP address and setting the count of IP address as one
        context.write(new Text(accessLogRecord.getClientAddress()), clientAddressCount);

    }


}
