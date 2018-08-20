package com.nitendragautam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
/**
 * Main Class and Entry Point for MapReduce Job where we define the Job Config
 * and set the Output Key and Value type for Mapper and Reducer
 */

public class LogAnalyticsHadoop {

    public static void main(String args[]) throws Exception{

Path inputPath = new Path(args[0]);
Path outputPath = new Path(args[1] + getTodaysDate());

Configuration conf = new Configuration(); //Hadoop Config

        Job mapreduceJob = Job.getInstance(conf ,"LogAnalyticsHadoop");
        mapreduceJob.setJarByClass(LogAnalyticsHadoop.class); //Define the Main Driver for MapReduce Job
        mapreduceJob.setMapperClass(LogAnalyticsMapper.class);
        mapreduceJob.setCombinerClass(LogAnalyticsReducer.class); //By Default Reducer will be the Combiner as not seperate Combiner Class is present
        mapreduceJob.setReducerClass(LogAnalyticsReducer.class);
        mapreduceJob.setOutputKeyClass(Text.class); //Output Key Type (IP address in this Application)
        mapreduceJob.setOutputValueClass(IntWritable.class); // Output Value Type (Count of IP address in this Application)
        FileInputFormat.addInputPath(mapreduceJob ,inputPath);
        FileOutputFormat.setOutputPath(mapreduceJob ,outputPath);
        System.exit(mapreduceJob.waitForCompletion(true) ? 0 :1);


    }

    private static String getTodaysDate(){
        DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy-hh-mm-ss");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,0); //Zero(0) means Todays Date

        return dateFormat.format(cal.getTime());
    }
}
