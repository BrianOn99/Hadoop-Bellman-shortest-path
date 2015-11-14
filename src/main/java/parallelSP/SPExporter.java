package parallelSP;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class SPExporter extends Configured implements Tool {

    public static class ExportMapper extends TableMapper<Text, Text> {
        public void map(ImmutableBytesWritable key, Result result, Context context)
                throws IOException, InterruptedException {

            int myDistance = Bytes.toInt(result.getValue(ParallelSP.familyMeta, ParallelSP.distance));
            context.write(new Text(key.get()), new Text(Integer.toString(myDistance)));
        }
    }

    public int run(String[] args) throws Exception {
        Scan scan = new Scan();
        scan.addColumn(ParallelSP.familyMeta, ParallelSP.distance);
        Filter filter = new SingleColumnValueFilter(
            ParallelSP.familyMeta, ParallelSP.distance,
            CompareFilter.CompareOp.NOT_EQUAL, ParallelSP.infDistance);
        scan.setFilter(filter);

        scan.setCaching(500);
        scan.setCacheBlocks(false);

        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(SPExporter.class);
        TableMapReduceUtil.initTableMapperJob(
            ParallelSP.TABLE_NAME,
            scan,
            ExportMapper.class,   // mapper
            Text.class,         // mapper output key
            IntWritable.class,  // mapper output value
            job);
        // set reduce task to zero to allow mapper directly writing to filesystem
        job.setNumReduceTasks(0);

        // job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
        return 0;
    }
}
