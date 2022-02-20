package it.unipi.hadoop.Rank;

import java.util.ArrayList;
import java.util.Iterator;

public class RankNode {
    private final ArrayList<Long> adiacencyList = new ArrayList<>();
    private long id;
    private double rank, epsilon;

    RankNode(){ epsilon = 0; }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        epsilon = Math.abs(this.rank - rank); this.rank = rank;
    }

    public void connectNode(long id){
        adiacencyList.add(id);
    }

    public long getArcsNumber(){ return adiacencyList.size(); }

    public Iterator<Long> getNodeIterator(){ return adiacencyList.iterator(); }

    public boolean parseRankNode(String str){
        String[] args = str.split("\\s+");
        if(args.length < 4)
            return false;
        id   = Long.parseLong(args[0]);
        rank = Double.parseDouble(args[1]);
        epsilon = Double.parseDouble(args[2]);
        long size = Long.parseLong(args[3]);
        if(args.length < 4 + size)
            return false;
        for(int i = 0; i < size; i++)
            connectNode(Long.parseLong(args[i + 4]));
        return true;
    }

    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append(id).append(" ").append(rank).append(" ").append(epsilon).append(" ").append(getArcsNumber());
        for(Iterator<Long> itr = getNodeIterator(); itr.hasNext();) {
            str.append(" ").append(itr.next());
        }

        return str.toString();
    }

    public void setEpsilon(double _epsilon){ epsilon = _epsilon; }
    public double getEpsilon(){ return epsilon; }
}
