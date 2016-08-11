package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//统计每个文档总词数,输出为:key = 单词@文档号 , value = 单词在该文档中出现次数/该文档总词数
public class WordCountInDocuments {
	
	public static class WordFrequenceMap extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\t");
			String[] wordAndFileName = strs[0].split("@");
			
			String val = wordAndFileName[0]+"@"+strs[1];
			
			context.write(new Text(wordAndFileName[1]), new Text(val));
		}
	}
	
	
	public static class WordFrequenceReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			List<String> list = new ArrayList<>();
			for(Text text :values){
				list.add(text.toString());
				String str = text.toString();
				String[] wordAndCount = str.split("@");
				sum += Integer.parseInt(wordAndCount[1]);
			}
			
			
			for(String text :list ){
				String str = text.toString();
				String[] wordAndCount = str.split("@");
				String outputKey = wordAndCount[0]+"@"+key;
				String outputValue = wordAndCount[1]+"/"+sum;
				context.write(new Text(outputKey), new Text(outputValue));
			}
			
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountInDocuments.class);
		job.setJobName("wordcount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(WordFrequenceMap.class);
		job.setReducerClass(WordFrequenceReduce.class);

		//指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(job, "/home/ubuntu/kmeans/ouputFrequence");
		
		//指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(job, new Path("/home/ubuntu/kmeans/ouputWordCount"));
		job.waitForCompletion(true);
	}

	
}
