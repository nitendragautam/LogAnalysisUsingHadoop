package com.nitendragautam.project.LogAnalytics;

import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
Defins the Job Config for MapReduce Job
 */
public class LogAnalytics {

    public static void main(String args[]) throws Exception{

Path inputPath = new Path(args[0]);
Path outputPath = new Path(args[1]);

Configuration conf = new Configuration(); //Hadoop Config
        Job mapreduceJob = Job.getInstance(conf ,"LogAnalytics");
        mapreduceJob.setJarByClass(com.nitendragautam.project.LogAnalytics.LogAnalytics.class);
        mapreduceJob.setMapperClass(com.nitendragautam.project.LogAnalytics.LogAnalyticsMapper.class);
        mapreduceJob.setCombinerClass(com.nitendragautam.project.LogAnalytics.LogAnalyticsReducer.class);
        mapreduceJob.setReducerClass(com.nitendragautam.project.LogAnalytics.LogAnalyticsReducer.class);
        mapreduceJob.setOutputKeyClass(Text.class); //Ouput Key Type
        mapreduceJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(mapreduceJob ,inputPath);
        FileOutputFormat.setOutputPath(mapreduceJob ,outputPath);
        System.exit(mapreduceJob.waitForCompletion(true) ? 0 :1);

        
    }
}
