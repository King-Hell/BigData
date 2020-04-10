import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
    private int sum=0;
    public void reduce(IntWritable key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        IntWritable i=new IntWritable(++sum);
        context.write(i,key);
    }
}

