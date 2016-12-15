package qst;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UV 
{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		private IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(" ");
			String ip = line[0];
			context.write(new Text(ip),one);
			}	
		}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable(1);
		int sum = 0;
		@Override
		public void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
				sum++;
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			result.set(sum);
			context.write(new Text("UV:"), result);
		}
	}
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"yangxi_pro1");
        job.setJarByClass(UV.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(Reduce.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return;
    }
}
