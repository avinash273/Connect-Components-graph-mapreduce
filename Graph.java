/*
CSE-6331-005
Project 3
Graph Analysis
Written by,
Avinash Shanker
Roll No: 1001668570
UTA ID: AXS8570
Date: 2-Mar-2019
*/

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable
{
    public Integer tag;
    public Integer group;
    public Integer VID;
    public ArrayList<Integer> adjacent;

    public Vertex()
    {
        this.tag = 0;
        this.group = 0;
        this.VID = 0;
        this.adjacent = new ArrayList<Integer>();
    }

    public Vertex(Integer tag, Integer group, Integer VID, ArrayList<Integer> AdjVec)
    {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = AdjVec;
    }


    public void readFields(DataInput input) throws IOException
    {
        tag = input.readInt();
        group = input.readInt();
        VID = input.readInt();
        adjacent.clear();
        IntWritable size = new IntWritable();
        size.readFields(input);
        for(short i = 0; i < size.get(); i++)
        {
            IntWritable VecAdj = new IntWritable();
            VecAdj.readFields(input);
            adjacent.add(VecAdj.get());
        }
    }

    public void write(DataOutput output) throws IOException
    {
        output.writeInt(tag);
        output.writeInt(group);
        output.writeInt(VID);
        IntWritable size = new IntWritable(adjacent.size());
        size.write(output);

        for(Integer VecAdj : adjacent)
        {
            output.writeInt(VecAdj);
        }
    }
}


public class Graph
{
    public static class MapperVID extends Mapper<Object, Text, IntWritable, Vertex>
    {

        @Override
        public void map(Object key, Text line, Context context)
                throws IOException, InterruptedException
        {
            String GetInput = line.toString();
            String[] SplitInp = GetInput.split(",");

            Integer VID = Integer.parseInt(SplitInp[0]);
            ArrayList<Integer> adjacent = new ArrayList<Integer>();
            for(short i = 1; i < SplitInp.length; i++)
            {
                adjacent.add(Integer.parseInt(SplitInp[i]));
            }

            Vertex ver = new Vertex(0, VID, VID, adjacent);
            context.write(new IntWritable(VID), ver);
        }
    }


    public static class MapperGrp extends Mapper<IntWritable, Vertex, IntWritable, Vertex>
    {

        @Override
        public void map(IntWritable key, Vertex vertex, Context context)
                throws IOException, InterruptedException
        {
            context.write(new IntWritable(vertex.VID), vertex);
            for(Integer adjacent : vertex.adjacent)
            {
                context.write(new IntWritable(adjacent),new Vertex(1,vertex.group,0,vertex.adjacent));
            }
        }
    }


    public static class GrpReducer extends Reducer<IntWritable, Vertex, IntWritable, Vertex>
    {
        @Override
        public void reduce(IntWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException
        {
            Integer GetGrp = Integer.MAX_VALUE;
            ArrayList<Integer> adjacent = new ArrayList<Integer>();
            for(Vertex ver : values)
            {
                Integer tag = ver.tag;
                if(tag == 0)
                {
                    adjacent = new ArrayList<Integer>(ver.adjacent);
                }

                GetGrp = min(GetGrp,ver.group);
            }

            Vertex assignVertex = new Vertex(0, GetGrp, key.get(), adjacent);
            context.write(new IntWritable(GetGrp), assignVertex);
        }

        public Integer min(Integer i, Integer j)
        {
            if(i < j)
            {
                return i;
            }
            else
                {
                    return j;
                }
        }
    }

    
    public static class MapperCount extends Mapper<IntWritable, Vertex, IntWritable, IntWritable>
    {
        @Override
        public void map(IntWritable key, Vertex vertex, Context context)
                throws IOException, InterruptedException
        {
            context.write(key, new IntWritable(1));
        }
    }

    public static class ReduceGroupCount extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int m = 0;

            for (IntWritable vertex : values)
            {
                m = m + vertex.get();
            }

            context.write(key, new IntWritable(m));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        Runtime.getRuntime().exec("rm -r output");
        Runtime.getRuntime().exec("rm -r temp");
        job.setJobName("MapTopology");
        job.setJarByClass(Graph.class);
        job.setMapperClass(MapperVID.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/f0"));
        job.waitForCompletion(true);

        for(short i = 0; i < 5; i++)
        {
            Job job1 = Job.getInstance();
            job1.setJobName("MapGroup");
            job1.setJarByClass(Graph.class);
            job1.setMapperClass(MapperGrp.class);
            job1.setReducerClass(GrpReducer.class);
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(Vertex.class);
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Vertex.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job1, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f" + (i+1)));
            job1.waitForCompletion(true);
        }

        Job job2 = Job.getInstance();
        job2.setJobName("MapCount");
        job2.setJarByClass(Graph.class);
        job2.setMapperClass(MapperCount.class);
        job2.setReducerClass(ReduceGroupCount.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }
}
/*
References:
http://lambda.uta.edu/cse6331/
*/