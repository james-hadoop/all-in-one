package com.james.hadoop.mapreduce.delete_duplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DeleteDuplicationJob {
    /**
     * Map:
     * 
     * inherited from Mapper
     * 
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();

        /**
         * map()
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }

    /**
     * Reduce:
     * 
     * inherited from Reducer
     *
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        /**
         * reduce()
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * Configuration
         */
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: DeleteDuplication <in> <in> <out>");
            System.exit(2);
        }

        /**
         * Job
         */
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Data Duplication");
        job.setJarByClass(DeleteDuplicationJob.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}