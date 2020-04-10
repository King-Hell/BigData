import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankOut extends Mapper<LongWritable, Text, DoubleWritable,Text> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String[] tuple=value.toString().split("\t");
        String page=tuple[0];
        double pr=Double.parseDouble(tuple[1]);
        context.write(new DoubleWritable(pr),new Text(page));
    }
}
