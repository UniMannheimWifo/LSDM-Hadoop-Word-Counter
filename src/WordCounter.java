

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounter extends Configured implements Tool {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private Text token = new Text();
		private static final LongWritable one = new LongWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String doc = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(doc);
			HashSet<Text> tokens = new HashSet<Text>();
			while (tokenizer.hasMoreElements()) {
				token.set(tokenizer.nextToken());
				tokens.add(token);
			}
			for (Text text : tokens) {
				context.write(text, one);
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable count: value) {
				sum += count.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Shakespeare");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		TextInputFormat.setInputPaths(job, "/shakespeare");
		TextOutputFormat.setOutputPath(job, new Path("/outputShakespeare"));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setJarByClass(WordCounter.class);

		return (job.waitForCompletion(true) == true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCounter(), args);

		System.exit(res);
	}
}
