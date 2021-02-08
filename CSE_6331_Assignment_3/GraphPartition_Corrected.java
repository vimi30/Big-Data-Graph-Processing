import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;  
    
    Vertex()
    {
        id = 0;
        adjacent = new Vector<Long>();
        centroid = 0;
        depth = 0;
        
    }

    Vertex(long id, Vector<Long> adjacent, long centroid, short depth)
    {
        this.id = id;
        this.adjacent = adjacent;
        this.centroid = centroid;
        this.depth = depth;
        
    }

    public void write ( DataOutput out ) throws IOException {
       
    
        out.writeLong(id);
        out.writeLong(centroid);
        out.writeShort(depth);

        if(adjacent!=null)
        {
            int adjacentSize = adjacent.size();

            out.writeInt(adjacentSize);

            for(int i=0; i<adjacentSize;i++)
            {
                out.writeLong(adjacent.get(i));
            }

        }else{
            out.writeInt(0);
        }

        


    }

    public void readFields ( DataInput in ) throws IOException {
        /*id = in.readLong();
        centroid = in.readLong();
        depth = in.readShort();
        int size = in.readInt();
        
        for(int i=0; i<size;i++)
        {
            try{
                adjacent.add(in.readLong());
            }catch(EOFException e){
            System.out.println("");
            System.out.println("End of file reached");
            break;
         } catch (IOException e) {
         }
        }*/
            
            
        id = in.readLong();
        centroid = in.readLong();
        depth = in.readShort();

        int size = in.readInt();

        adjacent.clear(); 
        
        for(int i=0;i<size;i++)
        {   
            adjacent.add(in.readLong());
        } 
    }
        
            
     

}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int count = 0;
   

    public static class GraphVertexMapper extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {

                     
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            
            long id = s.nextLong();
            Vector<Long> adjacent = new Vector<Long>();
            while(s.hasNext())
            {
                adjacent.add(s.nextLong());
            }

            long centroid = 0;
            if(count<10)
            {
                count++;
                centroid = id;
            }else
            {
                centroid = -1;
            }
            context.write(new LongWritable(id),new Vertex(id, adjacent, centroid, (short)0));

           /*System.out.print("id = " +id+ " adjacent = ");
           for(long v: adjacent)
           {
               System.out.print(v+",");
           }
           System.out.println(" centroid= "+ centroid);
            */
            s.close();
        }
    }

    public static class GraphCentroidMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
           
            context.write(new LongWritable(value.id),value);
            
            if(value.centroid>0)
            {
                //System.out.println("================================adjacentSize = "+value.adjacent.size()+"==============================");
                for(long v : value.adjacent)
                {
                    //System.out.println("In the value.adjacent");
                    context.write(new LongWritable(v),new Vertex(v, new Vector<Long>(), value.centroid,BFS_depth));
                }
            }
        }
    }

    public static class GraphCentroidReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {

            short min_depth = 1000;

            Vertex m = new Vertex(key.get(),   new Vector<Long>(),(long)-1, (short)0);

            for(Vertex v : values)
            {
                if(!v.adjacent.isEmpty())
                {
                    m.adjacent = v.adjacent;
                }

                if(v.centroid > 0 && v.depth <min_depth)
                {
                    min_depth = v.depth;
                    m.centroid = v.centroid; 
                }
                
            }
            m.depth = min_depth;

            
           
            context.write(key,m);
        }
    }
    
    public static class CentroidSumMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {

            
           
            context.write(new LongWritable(value.centroid),new LongWritable(1));
            
        }
    }

    public static class CentroidSumReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable(sum));
        }
    }
    

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("GraphVertexJob");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GraphVertexMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

       
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job2.setJarByClass(GraphPartition.class);
            job2.setMapperClass(GraphCentroidMapper.class);
            job2.setReducerClass(GraphCentroidReducer.class);

            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.setInputPaths(job2,new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/i"+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job3.setJarByClass(GraphPartition.class);
        job3.setMapperClass(CentroidSumMapper.class);
        job3.setReducerClass(CentroidSumReducer.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}
