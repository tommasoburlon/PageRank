package it.unipi.hadoop;

import it.unipi.hadoop.Compress.CompressMapper;
import it.unipi.hadoop.Compress.CompressReducer;
import it.unipi.hadoop.Compress.CompressWritable;
import it.unipi.hadoop.Parse.ParseMapper;
import it.unipi.hadoop.Parse.ParseReducer;
import it.unipi.hadoop.Rank.RankCombiner;
import it.unipi.hadoop.Rank.RankMapper;
import it.unipi.hadoop.Rank.RankReducer;
import it.unipi.hadoop.Rank.RankWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

    public static boolean step(String inputPath, String outputPath, Configuration conf) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);

        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(Main.class);

        KeyValueTextInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(RankMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(RankWritable.class);
        job.setCombinerClass(RankCombiner.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(RankWritable.class);

        return job.waitForCompletion(true);
    }

    public static boolean parse(String inputPath, String outputPath, Configuration conf) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);

        Job job = Job.getInstance(conf, "ParseGraph");
        job.setJarByClass(Main.class);

        KeyValueTextInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ParseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true);
    }

    public static boolean compress(String inputPath, String outputPath, Configuration conf) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);

        Job job = Job.getInstance(conf, "CompressGraph");
        job.setJarByClass(Main.class);

        KeyValueTextInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CompressMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompressWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(CompressReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setDouble("alpha", 0.1);
        conf.setLong("Nodes", 34493);
        conf.setStrings("link_regex", "(?<=\\[\\[).*?(?=]])");
        conf.setStrings("title_regex", "(?<=<title>).*?(?=</title>)");

        String inPath = "graph";
        String outDir = "output";
        final long PARSE = 1, COMPRESS = 2, RANK = 4;
        long ITERATIONS = 1, MOD = (PARSE | COMPRESS | RANK);

        for(int i = 0; i < args.length; i++){
            switch(args[i]){
                case "--input":
                    inPath = args[++i];
                    break;
                case "--output":
                    outDir = args[++i];
                    break;
                case "--alpha":
                    conf.setDouble("alpha", Double.parseDouble(args[++i]));
                    break;
                case "--nodes":
                    conf.setLong("Nodes", Long.parseLong(args[++i]));
                    break;
                case "--iterations":
                    ITERATIONS = Long.parseLong(args[++i]);
                    break;
                case "--no-parse":
                    MOD ^= PARSE;
                    break;
                case "--no-rank":
                    MOD ^= RANK;
                    break;
                case "--no-compress":
                    MOD ^= COMPRESS;
                    break;
                case "--title-regex":
                    conf.setStrings("title_regex", args[++i]);
                    break;
                case "--link-regex":
                    conf.setStrings("link_regex", args[++i]);
                    break;
                default:
                    System.out.println("flag " + args[i] + " not recognized");
                    break;

            }
        }

        if(MOD == (PARSE | RANK)){
            System.out.println("Sorry impossible to execute parsing and ranking in the same execution");
            return;
        }

        boolean ret = true;

        if((MOD & PARSE) != 0)
            ret = parse(inPath, outDir + "/parse", conf);
        if(ret && (MOD & COMPRESS) != 0)
            ret = compress((MOD & PARSE) == 0 ? inPath : outDir + "/parse", outDir + "/compress", conf);
        if(ret && (MOD & RANK) != 0) {
            for (int i = 0; i < ITERATIONS; i++) {
                String inputPath = (i > 0) ? (outDir + "/iter" + i + "/part-r-00000") : ((MOD & COMPRESS) == 0 ? inPath : outDir + "/compress");
                String outputPath = outDir + "/iter" + (i + 1);

                ret = step(inputPath, outputPath, conf);
                if(!ret)
                    break;
            }
        }

        if(!ret)
            System.out.println("[ERROR] something has gone wrong");
        else
            System.out.println("Finished.");

        //job.setCombinerClass(ParseReducer.class);
    }
}