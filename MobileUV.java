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

public class MobileUV {
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		private Text one = new Text("iPhone");
		private Text two = new Text("Android");
		public void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			String line = value.toString();
			Pattern pattern = Pattern.compile("iPhone");
			Pattern pattern2 = Pattern.compile("Android");
			Matcher matcher = pattern.matcher(line);
			Matcher matcher2 = pattern2.matcher(line);
			String[] line2 = value.toString().split(" ");
			String ip = line2[0];
			if(matcher.find()){
				context.write(new Text(ip),one);
				}
			else if(matcher2.find()){
				context.write(new Text(ip),two);
				}
			}		
		}
	public static class Reduce extends Reducer<Text,Text,Text,IntWritable>{
		int sum = 0;
		int count = 0;
		@Override
		public void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for (Text text : value) {
				if(text.equals(new Text("iPhone"))){
					sum++;
					break;
				}else if(text.equals(new Text("Android"))){
					count++;
					break;
				}
			}	
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("iPhoneUV:"), new IntWritable(sum));
			context.write(new Text("AndroidUV:"), new IntWritable(count));
		}
	}
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"yangxi_pro6");
        job.setJarByClass(MobileUV.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(Reduce.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return;
    }
}
