package parallelSP;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

public class ParallelSP {
    public static final String TABLE_NAME = "parallelSP";
    public static final byte [] familyMeta = Bytes.toBytes("meta");
    public static final byte [] familyNode = Bytes.toBytes("node");
    public static final byte [] color = Bytes.toBytes("color");
    public static final byte [] distance = Bytes.toBytes("distance");
    public static final byte [] grey = Bytes.toBytes("g");
    public static final byte [] white = Bytes.toBytes("w");
    public static final byte [] infDistance = Bytes.toBytes(Integer.MAX_VALUE);

    private static void setupTable() throws IOException {
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

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Error: Not enough command line argument");
            System.exit(-1);
        }
        setupTable();
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new MapImporter(), args);
        System.exit(exitCode);
    }
}
