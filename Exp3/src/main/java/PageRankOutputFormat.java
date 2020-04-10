import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

public class PageRankOutputFormat extends TextOutputFormat<DoubleWritable, Text> {

    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = ",";
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return isCompressed ? new PageRankRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator) : new PageRankRecordWriter(fileOut, keyValueSeparator);
    }
    protected static class PageRankRecordWriter<K, V> extends RecordWriter<K,V> {
        private static final byte[] NEWLINE;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public PageRankRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            this.keyValueSeparator = keyValueSeparator.getBytes(StandardCharsets.UTF_8);
        }

        public PageRankRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
            }

        }

        public synchronized void write(K key, V value) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            //修改输出顺序(value,key)
            if (!nullKey || !nullValue) {
                if (!nullValue) {
                    this.out.write("(".getBytes());
                    this.writeObject(value);
                }

                if (!nullKey && !nullValue) {
                    this.out.write(this.keyValueSeparator);
                }

                if (!nullKey) {
                    double d=((DoubleWritable)key).get();
                    DecimalFormat df = new DecimalFormat("0.0000000000");
                    this.out.write(df.format(d).getBytes());
                    this.out.write(")".getBytes());
                }

                this.out.write(NEWLINE);
            }
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            this.out.close();
        }

        static {
            NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
        }
    }
}
