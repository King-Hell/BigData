import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelationReducer extends Reducer<Text, TextBooleanWritable, Text, Text> {
    public static final boolean isParent=true;
    public static final boolean isChild=false;
    private boolean flag=true;
    public void reduce(Text key, Iterable<TextBooleanWritable> values,Context context)
            throws IOException, InterruptedException {
        if(flag) {
            context.write(new Text("grandchild"),new Text("grandparent"));
            flag=false;
        }
        Vector<String> children=new Vector<String>();
        Vector<String> parents=new Vector<String>();
        for (TextBooleanWritable x:values) {
            if(x.getBoolean()==isChild)
                children.add(x.getString());
            else
                parents.add(x.getString());
        }
        for (String child:children) {
            for(String parent:parents){
                context.write(new Text(child),new Text(parent));
            }
        }
    }
}

