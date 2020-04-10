import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
