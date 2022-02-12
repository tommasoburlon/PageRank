package it.unipi.hadoop.Compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CompressReducer extends Reducer<Text, CompressWritable, Text, NullWritable> {
    private Text response = new Text();
    private long N;
    @Override
    public void setup(Context ctx){
        N = ctx.getConfiguration().getLong("Nodes", 0);
    }
    @Override
    public void reduce(Text key, Iterable<CompressWritable> list, Context ctx) throws IOException, InterruptedException{
        StringBuilder res = new StringBuilder();
        long size = 0, id = 0;
        for(CompressWritable value : list){
            if(value.isSelf()){
                id = value.getId();
            }else{
                res.append(" ").append(value.getId());
                size++;
            }
        }

        response.set(id + " " + (1.0 / N) + " " + size + res);
        ctx.write(response, NullWritable.get());
    }
}
