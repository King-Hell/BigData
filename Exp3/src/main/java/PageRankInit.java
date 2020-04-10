import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankInit extends Mapper<LongWritable, Text,Text,Text> {
    //PageRank初始化pr值
    private static final String prInit="1.0\t";

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String[] tuple=value.toString().split("\t");//分割网页与链接
        String page=tuple[0];//网页
        String links=tuple[1];//链接
        context.write(new Text(page),new Text(prInit+links));
    }
}
