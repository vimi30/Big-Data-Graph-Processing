import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {

public static class MyMapper extends Mapper<Object,Text,LongWritable,LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long x = s.nextLong();
            long y = s.nextLong();
            context.write(new LongWritable(x),new LongWritable(y));
            s.close();
        }
    }

    public static class MyReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v: values) {
                count++;
            };
            context.write(key,new LongWritable(count));
        }
    }

    public static class MySecondMapper extends Mapper<Object,Text,LongWritable,LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\\t");
            long x = s.nextLong();
            long y = s.nextLong();
            long parity = 1;
            context.write(new LongWritable(y),new LongWritable(parity));
            s.close();
        }
    }

     public static class MySecondReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum+=v.get();
            };
            context.write(key,new LongWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {

     Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path("intermediate_out"));
        job.waitForCompletion(true);


        Job job2 = Job.getInstance();
        job2.setJobName("MyJobGroups");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(MySecondMapper.class);
        job2.setReducerClass(MySecondReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("intermediate_out"));
        //FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);

   }
}
