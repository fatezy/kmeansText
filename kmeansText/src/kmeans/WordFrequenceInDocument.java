package kmeans;

import java.io.IOException;

import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 提取文本 分词 key = 单词@文档号 , value = 单词在该文档中出现次数
 */
public class WordFrequenceInDocument {
	
	public static class WordFrequenceMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// 获取本次调用传递进来的数据所在的文件信息，先要获取所属切片信息
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			// 从切片信息中获取到文件路径及文件名
			String fileName = inputSplit.getPath().getName();
			context.write(new Text(line + "@" + fileName), new IntWritable(1));

		}
	}
	
		
		public static class WordFrequenceReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				context.write(key, new IntWritable(sum));
			}
		}

		
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(WordFrequenceInDocument.class);
			job.setJobName("WordFrequence");
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setMapperClass(WordFrequenceMap.class);
			job.setReducerClass(WordFrequenceReduce.class);

			//指定要处理的原始数据所存放的路径
			FileInputFormat.setInputPaths(job, "/home/ubuntu/kmeans/input");
			
			//指定处理之后的结果输出到哪个路径
			FileOutputFormat.setOutputPath(job, new Path("/home/ubuntu/kmeans/ouputFrequence"));
			job.waitForCompletion(true);
		}
	

}
