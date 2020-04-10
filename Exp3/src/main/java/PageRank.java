import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    private static final int ITERTIMES=10;
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
        FileSystem fs=FileSystem.get(conf);
        String outPathArg=otherArgs[otherArgs.length-1];
        Path[] paths=new Path[ITERTIMES+2];//记录所有输出目录数组
        Path outPath=new Path(outPathArg);
        paths[ITERTIMES+1]=outPath;
        Path initPath=new Path(outPathArg+"_Init");
        paths[0]=initPath;
        for(int i=1;i<=ITERTIMES;++i){
            paths[i]=new Path(outPathArg+"_Iter"+i);
        }
        for(int i=0;i<ITERTIMES+2;++i){
            if(fs.exists(paths[i]))
                fs.deleteOnExit(paths[i]);
        }
        fs.close();
        //初始化任务
        Job job1 = Job.getInstance(conf, "PageRankInit");
        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(PageRankInit.class);
        job1.setMapOutputKeyClass(Text.class);
        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1,initPath);
        job1.waitForCompletion(true);
        //迭代任务
        for(int i=1;i<=ITERTIMES;++i){
            Job job2=Job.getInstance(conf,"PageRankIter-"+i);
            job2.setJarByClass(PageRank.class);
            job2.setMapperClass(PageRankIterMapper.class);
            job2.setReducerClass(PageRankIterReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            FileInputFormat.addInputPath(job2,paths[i-1]);
            FileOutputFormat.setOutputPath(job2,paths[i]);
            job2.waitForCompletion(true);
        }
        //输出任务
        Job job3=Job.getInstance(conf,"PageRankOut");
        job3.setJarByClass(PageRank.class);
        job3.setMapperClass(PageRankOut.class);
        job3.setMapOutputKeyClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job3,paths[ITERTIMES]);
        FileOutputFormat.setOutputPath(job3,outPath);
        job3.setOutputFormatClass(PageRankOutputFormat.class);
        job3.setSortComparatorClass(PageRankComparator.class);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
