import java.io.*;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static HashSet<String> stopWords = new HashSet<>(820);
    private static Pattern pattern = Pattern.compile("\\w+");//匹配单词类字符

    public InvertedIndexMapper() throws IOException {
        super();
        if (stopWords.isEmpty()) {//初始化静态停词表
            InputStream fileIn = InvertedIndex.class.getResourceAsStream("stop_words_eng.txt");
            BufferedReader in = new BufferedReader(new InputStreamReader(fileIn));
            String str;
            while ((str = in.readLine()) != null) {
                stopWords.add(str);
            }
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //使用正则表达式匹配
        Matcher matcher=pattern.matcher(value.toString());
        while(matcher.find()){
            String word=matcher.group().toLowerCase();
            if (!stopWords.contains(word))//停词表不包含该词
                context.write(new Text(word), new Text(((FileSplit) context.getInputSplit()).getPath().getName()));
        }
    }
}
