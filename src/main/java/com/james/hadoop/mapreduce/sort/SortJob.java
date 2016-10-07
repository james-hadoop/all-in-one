package com.james.hadoop.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortJob {
    /**
     * Map:
     * 
     * inherited from Mapper
     * 
     */
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();

        /**
         * map()
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    /**
     * Reduce:
     * 
     * inherited from Reducer
     *
     */
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable linenum = new IntWritable(1);

        /**
         * reduce()
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            for (IntWritable val : values) {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get() + 1);
            }
        }
    }

    /**
     * Partition:
     * 
     * inherited from Partitioner
     */
    public static class Partition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int Maxnumber = 65223;
            int bound = Maxnumber / numPartitions + 1;
            int keyNumber = key.get();

            for (int i = 0; i < numPartitions; i++) {
                if (keyNumber < bound * (i + 1) && keyNumber >= bound * i) {
                    return i;
                }
            }

            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * Configuration
         */
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: Sort <in> <in> <in> <out>");
            System.exit(2);
        }

        /**
         * Job
         */
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Sort");
        job.setJarByClass(SortJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}