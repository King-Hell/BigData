import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        TreeMap<String,Integer> counts=new TreeMap<>();
        int sum=0;
        for(Text value:values){
            String fileName=value.toString();
            counts.putIfAbsent(fileName,0);
            counts.put(fileName,counts.get(fileName)+1);
            sum+=1;
        }
        StringBuilder out=new StringBuilder();
        for(String x:counts.keySet()) {
            out.append("<" + x + "," + counts.get(x) + ">;");
        }
        out.append("<total,"+sum+">.");
        context.write(key,new Text(out.toString()));
    }
}

