package it.unipi.hadoop.Compress;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompressMapper extends Mapper<LongWritable, Text, Text, CompressWritable> {
    private CompressWritable res = new CompressWritable();
    private Text id = new Text();
    private Pattern reg = Pattern.compile("\\[.*?]");
    @Override
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
        Matcher m;
        m = reg.matcher(value.toString());
        if(!m.find())
            throw new IOException("the input file was not parsed correctly");

        res.isSelf(true);
        res.setId(key.get());
        id.set(m.group());
        ctx.write(id, res);

        res.isSelf(false);
        while(m.find()){
            res.setId(key.get());
            id.set(m.group());
            ctx.write(id, res);
        }

    }
}
