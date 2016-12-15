package qst;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class BaiduPV {
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		int count = 0;
		public void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
			String line = value.toString();
			Pattern pattern = Pattern.compile("https://[\\w.]+baidu.com");
			Matcher matcher = pattern.matcher(line);
			if(matcher.find()){
				count ++;
				}
			}
			protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				context.write(new Text("BaiduPV:"), new IntWritable(count));
			}	
		}
	/*public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable(1);
		int sum = 0;
		@Override
		public void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for (IntWritable intWritable : value) {
				sum += intWritable.get();
			}
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			result.set(sum);
			context.write(new Text("BaiduPV:"), result);
		}
	}*/
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"yangxi_pro5");
        job.setJarByClass(BaiduPV.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return;
    }
}
