package com.nitendragautam.project.services;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper Class which takes log line as Input and Parses it
 */
public class LogAnalyticsMapper extends
        Mapper<LongWritable,Text  ,Text ,Text> {
    private IntWritable hour = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    AccessLogsParser accessLogsParser = new AccessLogsParser();

    public void map(LongWritable key ,Text  value ,Context context)
    throws IOException ,InterruptedException{
        String logRecord = value.toString();

        AccessLogRecord accessLogRecord = accessLogsParser.parseAccessLogs(logRecord);
context.write(new Text(accessLogRecord.getClientAddress()) ,new Text(accessLogRecord.getDateTime()));
    }


}
