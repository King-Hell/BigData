import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
    private static final double d=0.85;

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        double pr=0;
        String links="";
        for(Text value:values){
            String str=value.toString();
            if(str.startsWith("$")){
                links=str.substring(1);
                continue;
            }
            pr+=Double.parseDouble(str);
        }
        pr=1-d+d*pr;
        context.write(key,new Text(String.valueOf(pr)+"\t"+links));
    }
}

