import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelationMapper extends Mapper<LongWritable, Text, Text, TextBooleanWritable> {
    public static final boolean isParent=true;
    public static final boolean isChild=false;
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if(key.get()!=0) {
            StringTokenizer itr=new StringTokenizer(value.toString());
            String child=itr.nextToken();
            String parent=itr.nextToken();
            context.write(new Text(child),new TextBooleanWritable(parent,isParent));
            context.write(new Text(parent),new TextBooleanWritable(child,isChild));
        }
    }
}
