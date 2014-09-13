package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortApp {
	static final String URL = "hdfs://localhost:9000";
	static final String INPUT_PATH = "hdfs://localhost:9000/sort/input";
	static final String OUT_PATH = "hdfs://localhost:9000/sort/out";
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, SortApp.class.getSimpleName());
		
		FileSystem fileSystem = FileSystem.get(new URI(URL), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		// 1.1 input
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2 mapper
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 1.3 partition
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);

		// 1.4 sort, group

		// 1.5 combiner

		// 2.2 shuffle and sort, reducer
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// 2.3 output
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);

		// submit to JobTracker
		job.waitForCompletion(true);
		
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] splited = value.toString().split("\t");
			NewK2 k2 = new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1]));
			LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
			context.write(k2, v2);
		}
	}

	static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable> {
		@Override
		protected void reduce(NewK2 k2, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
			context.write(new LongWritable(k2.first), new LongWritable(k2.second));
		}
	}
	
	static class NewK2 implements WritableComparable<NewK2>{
		Long first;
		Long second;
		
		public NewK2() {}
		
		public NewK2(Long first, Long second) {
			this.first = first;
			this.second = second;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.first);
			out.writeLong(this.second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public int compareTo(NewK2 o) {
			Long minus = this.first - o.first;
			if (minus != 0L) {
				return minus.intValue();
			} else {
				return (int)(this.second - o.second);
			}
		}
		
		@Override
		public int hashCode() {
			return this.first.hashCode() + this.second.hashCode();
		}
		
		@Override
		public boolean equals(Object otherObj) {
			if (this == otherObj)
				return true;
			
			if (otherObj == null)
				return false;
			
			if (!(otherObj instanceof NewK2))
				return false;
			
			NewK2 other = (NewK2)otherObj;
			
			return first == other.first && second == other.second;
		}
	}
}
