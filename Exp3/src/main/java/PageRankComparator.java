import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;


public class PageRankComparator extends DoubleWritable.Comparator {
    @Override
    //重载比较方法为倒序
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        double thisValue = readDouble(b1, s1);
        double thatValue = readDouble(b2, s2);
        return -Double.compare(thisValue, thatValue);
    }
}
