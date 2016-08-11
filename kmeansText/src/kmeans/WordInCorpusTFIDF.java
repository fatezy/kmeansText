package kmeans;
//计算 tfidf,输出为:key = 单词@文档号 , value = TFIDF;

import java.io.File;
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


public class WordInCorpusTFIDF {
	
	
	public static class WordInCorpusTFIDFMap extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\t");
			String[] wordAndFileName = strs[0].split("@");
			String val = strs[1]+"/"+wordAndFileName[1];
			context.write(new Text(wordAndFileName[0]), new Text(val));
		}
	}


	public static class WordInCorpusTFIDFReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//a word appeal in all files (each file count only one)
				int wordCount = 0;
				File fileDir = new File("/home/ubuntu/kmeans/input");
				File[] srcFiles = fileDir.listFiles();
				int fileCount = srcFiles.length;
				
				List<String> list = new ArrayList<>();
				for(Text text:values){
					list.add(text.toString());
					wordCount++;
				}
				double idf = Math.log(fileCount/wordCount)/Math.log(10);
				
				for(String text : list){
					String[] strs = text.toString().split("/");
				if(strs.length == 3){
						double wordCountPerFile  = Double.parseDouble(strs[0]);
						double wordCountFile = Double.parseDouble(strs[1]);
						double tf = wordCountPerFile/wordCountFile;
						double wordWeigh = tf*idf;
						String outputKey = key+"@"+strs[2];
						String outputVal = String.valueOf(wordWeigh);
						context.write(new Text(outputKey), new Text(outputVal));
					}
				}
							
			
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordInCorpusTFIDFReduce.class);
		job.setJobName("WordTFIDF");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(WordInCorpusTFIDFMap.class);
		job.setReducerClass(WordInCorpusTFIDFReduce.class);

		//指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(job, "/home/ubuntu/kmeans/ouputWordCount");
		
		//指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(job, new Path("/home/ubuntu/kmeans/wordInCorpusTFIDF"));
		job.waitForCompletion(true);
	}


}
