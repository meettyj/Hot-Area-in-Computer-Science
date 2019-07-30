import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DataCleanerMapper
    extends Mapper<LongWritable, Text, Text, Text> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
//get key
String line = value.toString();
int startIndex = line.indexOf("\"id\"") + 7;
int endIndex = line.indexOf("\"name\"") - 3;
String k = line.substring(startIndex, endIndex);
//get value
startIndex = line.indexOf("\"pubs\"");
endIndex = line.indexOf("\"n_citation\"");
String v = line.substring(line.indexOf("\"name\""), startIndex) + line.substring(endIndex, line.length());
context.write(new Text(k), new Text(v));
}
}
