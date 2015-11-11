package parallelSP;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class MapImporter extends Configured implements Tool {

    public static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private Set<String> nodesSeen = new HashSet<>();

        public void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            String [] tokens = line.toString().split(" ");
            if(tokens.length != 3) {
                return;
            }

            // Extract each value
            byte [] from = Bytes.toBytes(tokens[0]);
            byte [] to = Bytes.toBytes(tokens[1]);
            byte [] weight = Bytes.toBytes(tokens[2]);
            Put put = new Put(from);
            put.add(ParallelSP.familyNode, to, weight);
            context.write(new ImmutableBytesWritable(from), put);

            // Note down what we have seen, and initialize them at cleanup()
            nodesSeen.add(tokens[0]);
            nodesSeen.add(tokens[1]);
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            for (String node: nodesSeen) {
                byte [] nodeName = Bytes.toBytes(node);
                Put put = new Put(nodeName);
                put.add(ParallelSP.familyMeta, ParallelSP.color, ParallelSP.white);
                put.add(ParallelSP.familyMeta, ParallelSP.distance, ParallelSP.infDistance);
                context.write(new ImmutableBytesWritable(nodeName), put);
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Error: Not enough command line argument");
            return -1;
        }
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(MapImporter.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(ImportMapper.class);
        TableMapReduceUtil.initTableReducerJob(ParallelSP.TABLE_NAME, null, job);
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
        return 0;
    }
}
