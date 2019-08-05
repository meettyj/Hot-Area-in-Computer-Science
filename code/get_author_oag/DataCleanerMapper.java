import java.io.*;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DataCleanerMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
//get key
String line = value.toString();
int idIndex = line.indexOf("\"id\"");
int nameIndex = line.indexOf("\"name\"");
if(idIndex < 0 || nameIndex < 0) return;
int indexEnd = idIndex;
while (line.charAt(indexEnd) != '}' && line.charAt(indexEnd) != ',') indexEnd++;
if (indexEnd-1 <= idIndex+7) return;
String k = line.substring(idIndex+7, indexEnd-1);
//get name
int nameEnd = nameIndex;
while (line.charAt(nameEnd) != '}' && line.charAt(nameEnd) != ',') nameEnd++;
if (nameEnd-1 <= nameIndex+9) return;
String name = line.substring(nameIndex+9, nameEnd-1);
//get h_index
int hIndexIndex = line.indexOf("\"h_index\"");
int S_h_index = 0;
if (hIndexIndex != -1) {
	int t = hIndexIndex;
    while (line.charAt(t) != '}' && line.charAt(t) != ',') t++;
	S_h_index = Integer.parseInt(line.substring(hIndexIndex+11, t));
}
//get n_pubs
int nPubsIndex = line.indexOf("\"n_pubs\"");
int S_n_pubs = 0;
if (nPubsIndex != -1) {
	int t = nPubsIndex;
    while (line.charAt(t) != '}' && line.charAt(t) != ',') t++;
	S_n_pubs = Integer.parseInt(line.substring(nPubsIndex+10, t));
}
//get n_citation
int nCitationIndex = line.indexOf("\"n_citation\"");
int S_n_citation = 0;
if (nCitationIndex != -1) {
	int t = nCitationIndex;
    while (line.charAt(t) != '}' && line.charAt(t) != ',') t++;
	S_n_citation = Integer.parseInt(line.substring(nCitationIndex+14, t));
}
//get value
double S_publication = (double)(S_h_index + S_n_pubs + S_n_citation)*0.3;
String v = name + "," + Double.toString(S_publication);
//context.write(new Text(k), new Text(v));
context.write(new Text(name), new DoubleWritable(S_publication));
}
}
