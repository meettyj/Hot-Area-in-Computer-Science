import java.io.IOException;
import java.util.*; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class DataCleanerReducer
extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Text value, Context context)
throws IOException, InterruptedException {
    //String line = value.toString();
	//context.write(key, new Text(getName(line)));
	//context.write(key, new Text(getHIndex(line)));
	//context.write(key, new Text(getNPubs(line)));
	//context.write(key, new Text(getTags(line)));
	//context.write(key, new Text(getNcitation(line)));
	//context.write(key, new Text(getOrgs(line)));
	context.write(key, value);
}
// get name
public String getName(String line){
	return line.substring(line.indexOf("\"name\"") + 9, line.indexOf("\"h_index\"") - 3);
}
// get h_index
public String getHIndex(String line){
	return line.substring(line.indexOf("\"h_index\"") + 11, line.indexOf("\"n_pubs\"") - 2);
}
// get n_pubs
public String getNPubs(String line){
	return line.substring(line.indexOf("\"n_pubs\"") + 10, line.indexOf("\"tags\"") - 2);
}
// get tags
public String getTags(String line){
	return line.substring(line.indexOf("\"tags\"") + 8, line.indexOf("\"n_citation\"") - 2);
}
// get n_citation
public String getNcitation(String line){
	return line.substring(line.indexOf("\"n_citation\"") + 14, line.indexOf("\"orgs\"") - 2);
}
// get orgs
public String getOrgs(String line){
	return line.substring(line.indexOf("\"orgs\"") + 8, line.length() - 1);
}
}