import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DataCleaner {
public static void main(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: MaxTemperature <input path> <output path>");
System.exit(-1);
}
Job job = new Job();
job.setJarByClass(DataCleaner.class);
job.setJobName("Data Cleaner");
FileInputFormat.addInputPaths(job, args[0]);
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(DataCleanerMapper.class);
job.setReducerClass(DataCleanerReducer.class);
job.setNumReduceTasks(1); 
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}