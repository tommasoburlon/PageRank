package it.unipi.hadoop.Rank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class RankCombiner extends Reducer<LongWritable, RankWritable, LongWritable, RankWritable> {
    private final RankWritable result = new RankWritable();

    public void reduce(LongWritable key, Iterable<RankWritable> list, Context ctx) throws IOException, InterruptedException {
        double res = 0;
        for(RankWritable itr : list){
            if(itr.isNode()){
                ctx.write(key, itr);
            }else{
                res += itr.getProbability();
            }
        }

        result.setProbability(res);
        ctx.write(key, result);
    }
}
