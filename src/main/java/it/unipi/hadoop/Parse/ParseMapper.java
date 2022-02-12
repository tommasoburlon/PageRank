package it.unipi.hadoop.Parse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text out = new Text();
    private Text matched = new Text();
    private final Text nullString = new Text("");
    private Pattern title;
    private Pattern link;
    @Override
    public void setup(Context ctx){
        title = Pattern.compile(ctx.getConfiguration().getStrings("title_regex", "(?<=<title>).*?(?=</title>)")[0]);
        link  = Pattern.compile(ctx.getConfiguration().getStrings("link_regex", "(?<=\\[\\[).*?(?=]])")[0]);
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher m;

        m = title.matcher(value.toString());
        if(!m.find())
            throw new IOException("this line does not contain a title, BAD INPUT");
        matched.set(m.group());
        m = link.matcher(value.toString());
        while (m.find()) {
            out.set(m.group());
            context.write(matched, nullString);
            context.write(out, matched);
        }
    }
}
