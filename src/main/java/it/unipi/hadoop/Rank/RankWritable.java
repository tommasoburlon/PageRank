package it.unipi.hadoop.Rank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class RankWritable implements Writable {
    RankNode node;
    double probability;
    boolean containNode;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(containNode);
        if(containNode){
            dataOutput.writeLong(node.getId());
            dataOutput.writeDouble(node.getRank());
            dataOutput.writeLong(node.getArcsNumber());
            for(Iterator<Long> itr = node.getNodeIterator(); itr.hasNext();)
                dataOutput.writeLong(itr.next());
        }else{
            dataOutput.writeDouble(probability);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        containNode = dataInput.readBoolean();
        if(containNode){
            node = new RankNode();
            node.setId(dataInput.readLong());
            node.setRank(dataInput.readDouble());
            long size = dataInput.readLong();
            for(int i = 0; i < size; i++)
                    node.connectNode(dataInput.readLong());
        }else{
            probability = dataInput.readDouble();
        }
    }

    public boolean isNode(){
        return containNode;
    }

    public void setNode(RankNode _node){
        containNode = true;
        node = _node;
    }

    public RankNode getNode(){ return node; }

    public void setProbability(double _probability){
        containNode = false;
        probability = _probability;
    }

    public double getProbability(){ return probability; }

    public String toString(){
        StringBuilder builder = new StringBuilder();

        if(this.isNode()) {
            builder.append(node.getRank()).append(" ").append(node.getEpsilon()).append(" ").append(node.getArcsNumber());
            for(Iterator<Long> itr = node.getNodeIterator(); itr.hasNext();)
                builder.append(" ").append(itr.next());
        }
        return builder.toString();
    }
}
