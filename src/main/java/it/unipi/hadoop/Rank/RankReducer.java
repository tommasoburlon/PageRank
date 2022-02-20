package it.unipi.hadoop.Rank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankReducer extends Reducer<LongWritable, RankWritable, LongWritable, RankWritable> {
    //private Text result = new Text();
    private final RankWritable result = new RankWritable();
    private double alpha;
    private long N;
    private boolean isFirst;
    @Override
    public void setup(Context ctx){
        alpha   = ctx.getConfiguration().getDouble("alpha",0.1);
        N       = ctx.getConfiguration().getLong("Nodes", 1000000);
    }

    @Override
    public void reduce(LongWritable key, Iterable<RankWritable> list, Context ctx) throws IOException, InterruptedException {
        double res = 0, rank;
        RankNode n = new RankNode();
        for(RankWritable value : list){
            if(value.isNode()){
                n = value.getNode();
            }else{
                res += value.getProbability();
            }
        }

        res = res * (1 - alpha) + ((double)1 / N) * alpha;
        n.setRank(res);
        result.setNode(n);
        ctx.write(key, result);
    }
}
