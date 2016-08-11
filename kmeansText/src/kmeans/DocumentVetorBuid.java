package kmeans;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import kmeans.WordInCorpusTFIDF.WordInCorpusTFIDFMap;
import kmeans.WordInCorpusTFIDF.WordInCorpusTFIDFReduce;

public class DocumentVetorBuid {
	public static class WordInCorpusTFIDFMap extends Mapper<LongWritable, Text, Text, Text> {
		
		//out: fileName   word:tfidf
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\t");
			String[] wordAndFileName =strs[0].split("@");
			String outKey = wordAndFileName[1];
			String outVal = wordAndFileName[0]+":"+strs[1];
			context.write(new Text(outKey), new Text(outVal));
		}
	}
	
	
	
	public static class WordInCorpusTFIDFReduce extends Reducer<Text, Text, Text, Text> {
		//choose center
		List<String> chooseFiles = new ArrayList<>();
		//centers
		Map<String,String> centers = new HashMap<>();
		
		@Override
		//choose init point
		protected void setup(Context context) throws IOException,
				InterruptedException {
			File fileDir = new File("/home/ubuntu/kmeans/input");
			File[] srcFiles = fileDir.listFiles();
			for(int i=0;i<5;i++){
				chooseFiles.add(srcFiles[i].getName());
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String outval = "";
			List<String> list = new ArrayList<>();
			
			
			for(Text text:values){
				list.add(text.toString());
				outval += text.toString()+"!";
			}
			
			if(chooseFiles.contains(key.toString()))
				centers.put(key.toString(), outval);
			context.write(key, new Text(outval));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			BufferedWriter  bufferedWriter = new BufferedWriter(new FileWriter(new File("/home/ubuntu/kmeans/center/center.txt")) );
			for(Entry<String, String> entry:centers.entrySet() ){
				bufferedWriter.write(entry.getKey()+" "+entry.getValue());
				bufferedWriter.newLine();
			}
			bufferedWriter.close();
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DocumentVetorBuid.class);
		job.setJobName("DocumentVetorBuid");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(WordInCorpusTFIDFMap.class);
		job.setReducerClass(WordInCorpusTFIDFReduce.class);

		//指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(job, "/home/ubuntu/kmeans/wordInCorpusTFIDF");
		
		//指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(job, new Path("/home/ubuntu/kmeans/DocumentVetorBuid"));
		job.waitForCompletion(true);
	}
	
	
	
		
	
}
