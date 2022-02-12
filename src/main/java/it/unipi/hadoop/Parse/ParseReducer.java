package it.unipi.hadoop.Parse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParseReducer extends Reducer<Text, Text, Text, NullWritable>{
    private Text textID = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder res = new StringBuilder();
        for (Text val : values) {
            if(!val.toString().equals(""))
                res.append(" [").append(val).append("]");
        }
        textID.set("[" + key + "]" + res);
        context.write(textID, NullWritable.get());
    }
}
