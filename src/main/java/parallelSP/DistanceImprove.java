package parallelSP;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class DistanceImprove extends Configured implements Tool {
    static enum MyCounter { NEGDIST, POSDIST };

    public static class NodeMapper extends TableMapper<Text, IntWritable> {

        public void map(ImmutableBytesWritable key, Result result, Context context)
                throws IOException, InterruptedException {
            int myDistance = Bytes.toInt(result.getValue(ParallelSP.familyMeta, ParallelSP.distance));

            /* emit itself. Use negative value to make it special
             * Given that the edge weight greater or eq 0, it is safe to do so
             */
            context.write(new Text(key.get()), new IntWritable(-myDistance));

            // if itself is modified, emit distance to child
            boolean modifiedLast = Bytes.toBoolean(result.getValue(ParallelSP.familyMeta, ParallelSP.modifiedLast));
            if (!modifiedLast) {
                return;
            }
            for (KeyValue child: result.raw()) {
                if (Bytes.compareTo(child.getFamily(), ParallelSP.familyNode) == 0) {
                    int distToChild = Bytes.toInt(child.getValue());
                    if (distToChild == Integer.MAX_VALUE)
                        throw new RuntimeException("Encounter node with INF distance");
                    context.write(new Text(child.getQualifier()),
                                  new IntWritable(myDistance + distToChild));
                }
            }
            /*
            Map<byte [], byte []> childs = result.getFamilyMap(ParallelSP.familyMeta);
            for (Map.Entry<byte [], byte []>: child: childs.entrySet()) {
                context.write(new Text(child.getQualifier()), new IntWritable(Bytes.toInt(child.getValue())));
            }
            */
        }
    }

    public static class NodeReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> dists, Context context)
                throws IOException, InterruptedException {
            int minDist = Integer.MAX_VALUE;
            int origDist = Integer.MAX_VALUE;
            for (IntWritable x: dists) {
                int y = x.get();
                if (y <= 0) {
                    origDist = -y;
                    context.getCounter(MyCounter.NEGDIST).increment(1);
                } else if (y < minDist) {
                    minDist = y;
                    context.getCounter(MyCounter.POSDIST).increment(1);
                }
            }
            if (minDist < origDist) {
                Put put = new Put(Bytes.toBytes("gg" + key.toString()));
                put.add(ParallelSP.familyMeta, ParallelSP.distance ,Bytes.toBytes(minDist));
                context.write(null, put);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Scan scan = new Scan();

        /*
        // Only process nodes that are updated last time
        Filter filter = new SingleColumnValueFilter(
            ParallelSP.familyMeta, ParallelSP.modifiedLast,
            CompareFilter.CompareOp.EQUAL, ParallelSP.yes);
        scan.setFilter(filter);
        */
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = new Job(conf);
        job.setJarByClass(DistanceImprove.class);
        job.setMapperClass(NodeMapper.class);
        TableMapReduceUtil.initTableMapperJob(
            ParallelSP.TABLE_NAME,
            scan,
            NodeMapper.class,   // mapper
            Text.class,         // mapper output key
            IntWritable.class,  // mapper output value
            job);
        TableMapReduceUtil.initTableReducerJob(
            ParallelSP.TABLE_NAME,
            NodeReducer.class,
            job);

        job.waitForCompletion(true);
        return 0;
    }
}
