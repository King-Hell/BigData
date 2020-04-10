import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable i=new IntWritable(Integer.parseInt(value.toString()));
        context.write(i,NullWritable.get());
    }
}
