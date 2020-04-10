import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
