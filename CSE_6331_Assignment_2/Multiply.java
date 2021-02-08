import java.io.*;
import java.util.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;


class Elem implements Writable {
  public short tag;  
  public int index;  
  public double value;
  
  Elem(){}

  Elem(short t, int i, double v)
  {
    this.tag=t;
    this.index = i;
    this.value = v;
  }

  
  public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    
  public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
  }
}

class Pair implements WritableComparable<Pair> {
  public int i;
  public int j;

  Pair(){}

  Pair(int i, int j)
  {
    this.i = i;
    this.j = j;
  }

  
  public void write ( DataOutput out ) throws IOException {
      out.writeInt(i);
      out.writeInt(j);
  }

  
  public void readFields ( DataInput in ) throws IOException {
      i = in.readInt();
      j = in.readInt();      
  }

  
  public int compareTo(Pair pair) {
    
    if(i>pair.i )
    {
        return 1;

    }else if(i<pair.i)
    {
        return -1;

    }else if(j>pair.j)
    {
        return 1;

    }else if(j<pair.j)
    {
        return -1;

    }else
    {
        return 0;
    }
    

  }

  
 
  public String toString() {

      return String.valueOf(i) + " " + String.valueOf(j);
  }
}




public class Multiply {

public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int Mi = s.nextInt();
            int Mj = s.nextInt();
            double Mv = s.nextDouble();
            context.write(new IntWritable(Mj),new Elem((short)0,Mi,Mv));
            s.close();
        }
    }


    
public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int Ni= s.nextInt();
            int Nj = s.nextInt();
            double Nv = s.nextDouble();
            context.write(new IntWritable(Ni),new Elem((short)1,Nj,Nv));
            s.close();
        }
    }


 public static class MatrixJoinReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        
        static Vector<Elem> Matrix_M = new Vector<Elem>();
        static Vector<Elem> Matrix_N = new Vector<Elem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {

            

            Configuration conf = context.getConfiguration();

            Matrix_M.clear();
            Matrix_N.clear();
            for (Elem e: values)
            {
                Elem tmpCp = ReflectionUtils.newInstance(Elem.class,conf);
                ReflectionUtils.copy(conf,e,tmpCp);// making a deep copy of the elem  object and storing them into the Vector<Elem>
                
                if (tmpCp.tag == 0)
                {   
                    Matrix_M.add(tmpCp);
                }
                if(tmpCp.tag == 1)
                {
                    Matrix_N.add(tmpCp);

                } 
                

            }
                
            for ( Elem a: Matrix_M )
            {
                for ( Elem b: Matrix_N )
                {
                    context.write(new Pair(a.index,b.index),new DoubleWritable(a.value*b.value));
                }
                    
            }
                
        }
    }

    public static class MultiplicationMapper extends Mapper<Object,Text,Pair,DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\\s");
            int x = s.nextInt();
            int y = s.nextInt();
            double z = s.nextDouble();
            context.write(new Pair(x,y),new DoubleWritable(z));
            s.close();
        }
    }

    public static class MultiplicationReducer extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v: values) {
                sum += v.get();
            };
            context.write(key,new DoubleWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {

        Job job = Job.getInstance();
        job.setJobName("JoinJob");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        job.setReducerClass(MatrixJoinReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MatrixMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MatrixNMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MultiplicationJob");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MultiplicationMapper.class);
        job2.setReducerClass(MultiplicationReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);

    }
}
