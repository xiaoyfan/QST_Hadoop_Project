package qst;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DaysRemain {
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(" ");
			String ip = line[0];
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			String filename = filesplit.getPath().getParent().getName();
			Configuration conf = context.getConfiguration();
			String pathm = conf.get("day1");
			String pathn = conf.get("day2");
			if(filename.equals(pathm)){
				context.write(new Text(ip), new Text("a"));
			}else if(filename.equals(pathn)){
				context.write(new Text(ip), new Text("b"));
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		Double count = 0.0 ;
		Double sum = 0.0;
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			boolean IPA = false;
			boolean IPB = false;
			for(Text s : value){
				if(s.equals(new Text("a"))){
					IPA = true;
				}else if(s.equals(new Text("b"))){
					IPB = true;
				}
			}
			if(IPA == true && IPB == true){
				count++;
			}
			if(IPA == true){
				sum++;
			}
		}
		public void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Double result;
			result = count / sum ;
			context.write(new Text("Days remain"), new Text(result+" "+count+" "+sum));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("day1", args[2]);
		conf.set("day2", args[3]);
        Job job = Job.getInstance(conf,"yangxi_pro3");
        job.setJarByClass(DaysRemain.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(Reduce.class);
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return;
	}
}
