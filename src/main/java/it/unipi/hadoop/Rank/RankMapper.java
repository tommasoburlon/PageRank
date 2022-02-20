package it.unipi.hadoop.Rank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

public class RankMapper extends Mapper<LongWritable, Text, LongWritable, RankWritable>{
    private final RankWritable result = new RankWritable();
    private final LongWritable id = new LongWritable();
    @Override
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        RankNode n = new RankNode();
        boolean ret = n.parseRankNode(value.toString());
        if(!ret)
            throw new IOException("Graph node is not parsed correctly");
        double P = n.getRank() / n.getArcsNumber();

        result.setProbability(P);

        for(Iterator<Long> itr = n.getNodeIterator(); itr.hasNext();){
            id.set(itr.next());
            ctx.write(id, result);
        }

        result.setNode(n);
        id.set(n.getId());
        ctx.write(id, result);
    }
}
