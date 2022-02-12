package it.unipi.hadoop.Compress;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompressWritable implements Writable {
    private long id;
    private boolean selfId;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(selfId);
        dataOutput.writeLong(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        selfId = dataInput.readBoolean();
        id     = dataInput.readLong();
    }

    public long getId(){ return id; }
    public void setId(long _id){ id = _id; }
    public boolean isSelf(){ return selfId; }
    public void isSelf(boolean _selfId){ selfId = _selfId; }
}
