import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankIterMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        ArrayWritable
        String[] tuple=value.toString().split("\t");//分割网页 pr 链接
        String page=tuple[0];//网页
        double pr=Double.parseDouble(tuple[1]);//pr
        String links=tuple[2];
        context.write(new Text(page),new Text("$"+links));//输出网页与链接
        String[] linkTuple=tuple[2].split(",");//分割对外链接
        for(String link:linkTuple){
            double aPr=pr/linkTuple.length;//计算当前网页对目标网页的pr贡献
            context.write(new Text(link),new Text(String.valueOf(aPr)));
        }
    }
}
