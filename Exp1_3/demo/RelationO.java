import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

public class TextBooleanWritable implements Writable {
    private String str;
    private boolean bool;
    TextBooleanWritable(){

    }
    TextBooleanWritable(String str, boolean bool){
        this.str=str;
        this.bool=bool;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(str);
        dataOutput.writeBoolean(bool);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.str=dataInput.readUTF();
        this.bool=dataInput.readBoolean();
    }

    public String getString(){
        return this.str;
    }

    public boolean getBoolean(){
        return this.bool;
    }
}

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

public class Relation {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: sort <in> [<in>...] <out>");
            System.exit(2);
        }
        FileSystem fs=FileSystem.get(conf);
        Path outPath=new Path(otherArgs[otherArgs.length-1]);
        if(fs.exists(outPath)){
            fs.deleteOnExit(outPath);
        }
        fs.close();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Relation.class);
        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextBooleanWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

