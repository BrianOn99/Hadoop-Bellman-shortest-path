package parallelSP;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class MapImporter extends Configured implements Tool {
    private static final String TABLE_NAME = "parallelSP";
    public static final byte [] familyMeta = Bytes.toBytes("meta");
    public static final byte [] familyNode = Bytes.toBytes("node");
    public static final byte [] color = Bytes.toBytes("color");
    public static final byte [] grey = Bytes.toBytes("g");
    public static final byte [] white = Bytes.toBytes("w");

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
            put.add(familyNode, to, weight);
            context.write(new ImmutableBytesWritable(from), put);

            nodesSeen.add(tokens[0]);
            nodesSeen.add(tokens[1]);
        }

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
                  /*
            super.setup(context);
            try {
                this.table = new HTable(new HBaseConfiguration(job), "SPmap");
            } catch (IOException e) {
                throw new RuntimeException("Failed HTable construction", e);
            }
            */
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            for (String node: nodesSeen) {
                byte [] nodeName = Bytes.toBytes(node);
                Put put = new Put(nodeName);
                put.add(familyMeta, color, white);
                context.write(new ImmutableBytesWritable(nodeName), put);
            }
        }
    }

    private void setupTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }
        HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
        htd.addFamily(new HColumnDescriptor("meta"));
        htd.addFamily(new HColumnDescriptor("node"));
        admin.createTable(htd);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }
        setupTable();
        Configuration conf = getConf();
        Job job = new Job(conf);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(ImportMapper.class);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, null, job);
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseConfiguration(),
                new MapImporter(), args);
        System.exit(exitCode);
    }
}
