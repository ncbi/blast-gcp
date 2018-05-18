/*===========================================================================
 *
 *                            PUBLIC DOMAIN NOTICE
 *               National Center for Biotechnology Information
 *
 *  This software/database is a "United States Government Work" under the
 *  terms of the United States Copyright Act.  It was written as part of
 *  the author's official duties as a United States Government employee and
 *  thus cannot be copyrighted.  This software/database is freely available
 *  to the public for use. The National Library of Medicine and the U.S.
 *  Government have not placed any restriction on its use or reproduction.
 *
 *  Although all reasonable efforts have been taken to ensure the accuracy
 *  and reliability of the software and data, the NLM and the U.S.
 *  Government do not and cannot warrant the performance or results that
 *  may be obtained by using this software or data. The NLM and the U.S.
 *  Government disclaim all warranties, express or implied, including
 *  warranties of performance, merchantability or fitness for any particular
 *  purpose.
 *
 *  Please cite the author in any work or product based on this material.
 *
 * ===========================================================================
 *
 */

package gov.nih.nlm.ncbi.blastjni;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.json.JSONObject;

public final class BLAST_DRIVER implements Serializable {
    private static BLAST_SETTINGS settings;
    private static SparkSession sparksession;
    // Only one spark context allowed per JVM
    private static JavaSparkContext javasparkcontext;

    public static boolean init(final String[] args) {
        if (args.length != 1) {
            System.out.println("settings json-file missing");
            return false;
        }
        final String ini_path = args[0];

        final String appName = "experiment_mike";
        settings = BLAST_SETTINGS_READER.read_from_json(ini_path, appName);
        System.out.println(String.format("settings read from '%s'", ini_path));
        if (!settings.valid()) {
            System.out.println(settings.missing());
            return false;
        }
        System.out.println(settings.toString());

        SparkSession.Builder builder = new SparkSession.Builder();
        builder.appName(settings.appName);

        SparkConf conf = new SparkConf();
        conf.setAppName(settings.appName);

        // GCP NIC has 2 gbit/sec/vCPU, 16 gbit max, ~8 gbit for single stream
        // LZ4 typically saturates at 800MB/sec
        // I'm only seeing ~2.4 gbit/core, so LZ4 a win
        conf.set("spark.broadcast.compress", "true");
        // GCP non-ssd persistent disk is <= 120MB/sec
        conf.set("spark.shuffle.compress", "true");

        int num_db_partitions = 1234;
        conf.set("spark.default.parallelism", Integer.toString(num_db_partitions));
        conf.set("spark.dynamicAllocation.enabled", Boolean.toString(settings.with_dyn_alloc));
        conf.set("spark.eventLog.enabled", "false");

        if (settings.num_executor_cores > 0)
            conf.set("spark.executor.cores", String.format("%d", settings.num_executor_cores));
        // These will appear in
        // executor:/var/log/hadoop-yarn/userlogs/applica*/container*/stdout
        // FIX: +UseParallelGC ? Increase G1GC latency?
        //        conf.set("spark.executor.extraJavaOptions", "-XX:+PrintCompilation -verbose:gc");
        if (settings.num_executors > 0)
            conf.set("spark.executor.instances", String.format("%d", settings.num_executors));
        if (!settings.executor_memory.isEmpty())
            conf.set("spark.executor.memory", settings.executor_memory);

        conf.set("spark.locality.wait", settings.locality_wait);

        // -> process, node, rack, any
        if (settings.scheduler_fair)
            conf.set("spark.scheduler.mode", "FAIR"); // FIX, need fairscheduler.xml
        conf.set("spark.shuffle.reduceLocality.enabled", "false");

        conf.set("spark.sql.shuffle.partitions", Integer.toString(num_db_partitions));
        // conf.set("spark.sql.streaming.schemaInference", "true");
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        conf.set("spark.sql.warehouse.dir", warehouseLocation);

        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        conf.set("spark.streaming.receiver.maxRate", String.format("%d", settings.max_backlog));

        conf.set("spark.ui.enabled", "true");

        builder.config(conf);
        System.out.println("Configuration is");
        System.out.println("----------------");
        System.out.println(conf.toDebugString().replace("\n", "\n  "));
        System.out.println();
        builder.enableHiveSupport();

        sparksession = builder.getOrCreate();
        javasparkcontext = new JavaSparkContext(sparksession.sparkContext());
        javasparkcontext.setLogLevel(settings.spark_log_level);

        // send the given files to all nodes
        List<String> files_to_transfer = new ArrayList<>();
        files_to_transfer.add("libblastjni.so");
        files_to_transfer.add("log4j.properties");
        for (String a_file : files_to_transfer) {
            javasparkcontext.addFile(a_file);
        }

        return true;
    }

    private static boolean make_partitions() {
        StructType parts_schema = StructType.fromDDL("db string, partition_num int");

        int num_db_partitions = 1234;

        ArrayList<Row> data = new ArrayList<Row>(num_db_partitions);

        for (int i = 0; i < num_db_partitions; i++) {
            Row r = RowFactory.create("nt", Integer.valueOf(i));
            data.add(r);
        }

        Dataset<Row> parts = sparksession.createDataFrame(data, parts_schema);
        Dataset<Row> blast_partitions =
            parts
            .repartition(num_db_partitions, parts.col("partition_num"))
            .persist(StorageLevel.MEMORY_AND_DISK());

        blast_partitions.show();
        blast_partitions.createOrReplaceTempView("blast_partitions");
        System.out.println("blast_partitions is: " + Arrays.toString(blast_partitions.inputFiles()));

        return true;
    }

    private static DataStreamWriter<Row> make_prelim_stream() {
        System.out.println("making prelim_stream");

        StructType queries_schema =
            // StructType.fromDDL("RID string, db string, query_seq string, timestamp_hdfs string");
            StructType.fromDDL(
                    "protocol string, "
                    + "RID string, "
                    + "db_tag string, "
                    + // nt_50M.20180502_1
                    // volumes // optional for filtering by tax-id
                    "top_N_prelim int, "
                    + "top_N_traceback int, "
                    + "query_seq string, "
                    + "query_url string, "
                    + "program string, "
                    + "blast_params string, "
                    + "StartTime timestamp");

        DataStreamReader query_stream = sparksession.readStream();
        query_stream.format("json");
        query_stream.option("maxFilesPerTrigger", settings.max_backlog);
        query_stream.option("multiLine", true);
        query_stream.option("mode", "FAILFAST");
        query_stream.option("includeTimestamp", true);
        query_stream.schema(queries_schema);

        Dataset<Row> queries = query_stream.json(settings.hdfs_source_dir);
        System.out.print("queries schema is ");
        queries.printSchema();
        queries.createOrReplaceTempView("queries");

        Dataset<Row> joined =
            sparksession.sql(
                    "select RID, queries.db as db, partition_num, "
                    + "query_seq, timestamp_hdfs "
                    + "from queries, blast_partitions "
                    + "where queries.db=blast_partitions.db "
                    + "distribute by partition_num");
        joined.createOrReplaceTempView("joined");
        System.out.print("joined schema is ");
        joined.printSchema();

        Integer top_n = settings.top_n;
        String jni_log_level = settings.jni_log_level;
        String hsp_result_dir = settings.hdfs_result_dir + "/hsps";
        String db_location = "/tmp/blast/db"; // settings.db_location;

        Dataset<Row> prelim_search_results =
            joined
            .flatMap( // FIX: Make a functor
                    (FlatMapFunction<Row, String>)
                    inrow -> {
                        Logger logger = LogManager.getLogger(BLAST_DRIVER.class);

                        String rid = inrow.getString(inrow.fieldIndex("RID"));
                        logger.log(Level.DEBUG, "Flatmapped RID " + rid);
                        String db = inrow.getString(inrow.fieldIndex("db"));
                        int partition_num = inrow.getInt(inrow.fieldIndex("partition_num"));
                        String query_seq = inrow.getString(inrow.fieldIndex("query_seq"));
                        logger.log(
                                Level.DEBUG,
                                String.format("in flatmap %d %s", partition_num, db_location));

                        BLAST_REQUEST requestobj = new BLAST_REQUEST();
                        requestobj.id = rid;
                        requestobj.query_seq = query_seq;
                        requestobj.query_url = "";
                        requestobj.db = db;
                        requestobj.params = "blastn";
                        requestobj.program = "blastn";
                        requestobj.top_n = top_n;
                        BLAST_PARTITION partitionobj =
                            new BLAST_PARTITION(db_location, "nt_50M", partition_num, false);

                        BLAST_LIB blaster = new BLAST_LIB();
                        // BLAST_LIB_SINGLETON.get_lib(partitionobj, settings_closure);
                        logger.log(Level.DEBUG, "<row> is :" + inrow.mkString(":"));

                        List<String> hsp_json;
                        if (blaster != null) {
                            BLAST_HSP_LIST[] search_res =
                                blaster.jni_prelim_search(partitionobj, requestobj, jni_log_level);

                            logger.log(
                                    Level.INFO,
                                    String.format(" prelim returned %d hsps to Spark", search_res.length));

                            hsp_json = new ArrayList<>(search_res.length);
                            for (BLAST_HSP_LIST S : search_res) {
                                byte[] encoded = Base64.getEncoder().encode(S.hsp_blob);
                                String b64blob = new String(encoded, StandardCharsets.UTF_8);
                                JSONObject json = new JSONObject();
                                json.put("RID", rid);
                                json.put("db", db);
                                json.put("partition_num", partition_num);
                                json.put("oid", S.oid);
                                json.put("max_score", S.max_score);
                                json.put("query_seq", query_seq);
                                json.put("hsp_blob", b64blob);

                                hsp_json.add(json.toString());
                                // logger.log(Level.INFO, "json is " + json.toString());
                            }
                            logger.log(
                                    Level.INFO, String.format("hsp_json has %d entries", hsp_json.size()));
                        } else {
                            logger.log(Level.ERROR, "NULL blaster library");
                            hsp_json = new ArrayList<>();
                            hsp_json.add("null blaster ");
                        }
                        return hsp_json.iterator();
                    },
                          Encoders.STRING())
                              .toDF("hsp_json");

        prelim_search_results.explain(true);
        // prelim_search_results.createOrReplaceTempView("prelim_search_results");
        // Don't use coalesce here, it'll group previous work onto one worker
        Dataset<Row> prelim_search_1 = prelim_search_results.repartition(1);
        // FIX: Partition by RID, which is in JSON string at the moment

        DataStreamWriter<Row> prelim_dsw = prelim_search_1.writeStream();

        DataStreamWriter<Row> topn_dsw =
            prelim_dsw
            .foreach( // FIX: Make a separate function
                    new ForeachWriter<Row>() {
                        private long partitionId;
                        private int recordcount = 0;
                        private FileSystem fs;
                        private Logger logger;

                        // So we can efficiently compute topN scores by RID
                        HashMap<String, TreeMap<Integer, ArrayList<String>>> score_map;

                        @Override
                        public boolean open(long partitionId, long version) {
                            score_map = new HashMap<>();
                            this.partitionId = partitionId;
                            logger = LogManager.getLogger(BLAST_DRIVER.class);
                            try {
                                Configuration conf = new Configuration();
                                fs = FileSystem.get(conf);
                            } catch (IOException e) {
                                System.err.println(e);
                                return false;
                            }

                            logger.log(Level.DEBUG, String.format("ps open %d %d", partitionId, version));
                            if (partitionId != 0)
                                logger.log(
                                        Level.DEBUG, String.format(" *** not partition 0 %d ??? ", partitionId));
                            return true;
                        } // open

                        @Override
                        public void process(Row value) {
                            ++recordcount;
                            logger.log(Level.DEBUG, String.format(" in process %d", partitionId));
                            logger.log(Level.DEBUG, "  " + value.mkString(":").substring(0, 50));
                            String line = value.getString(0);
                            logger.log(Level.DEBUG, "  line is " + line);

                            JSONObject json = new JSONObject(line);
                            String rid = json.getString("RID");
                            int max_score = json.getInt("max_score");
                            // String db=json.getString("db");
                            // int partition_num=json.getInt("partition_num");
                            // int oid=json.getInt("oid");
                            // String query_seq=json.getString("query_seq");
                            // String hsp_blob=json.getString("hsp_blob");

                            logger.log(Level.DEBUG, String.format(" max_score is %d", max_score));

                            if (!score_map.containsKey(rid)) {
                                TreeMap<Integer, ArrayList<String>> tm =
                                    new TreeMap<Integer, ArrayList<String>>(Collections.reverseOrder());
                                score_map.put(rid, tm);
                            }

                            TreeMap<Integer, ArrayList<String>> tm = score_map.get(rid);

                            // FIX optimize: early cutoff if tm.size>topn
                            if (!tm.containsKey(max_score)) {
                                ArrayList<String> al = new ArrayList<String>();
                                tm.put(max_score, al);
                            }
                            ArrayList<String> al = tm.get(max_score);

                            al.add(line);
                            tm.put(max_score, al);
                            score_map.put(rid, tm);
                        }

                        @Override
                        public void close(Throwable errorOrNull) {
                            logger.log(Level.DEBUG, "close results:");
                            logger.log(Level.DEBUG, "---------------");
                            logger.log(Level.DEBUG, String.format(" saw %d records", recordcount));
                            logger.log(Level.DEBUG, String.format(" hashmap has %d", score_map.size()));

                            for (String rid : score_map.keySet()) {
                                TreeMap<Integer, ArrayList<String>> tm = score_map.get(rid);
                                // JSON records tend to be either ~360 or ~1,200 bytes
                                StringBuilder output = new StringBuilder(tm.size() * 400);
                                int i = 0;
                                for (Integer score : tm.keySet()) {
                                    if (i < top_n) {
                                        ArrayList<String> al = tm.get(score);
                                        for (String line : al) {
                                            logger.log(
                                                    Level.DEBUG,
                                                    String.format(
                                                        "  rid=%s, score=%d, i=%d,\n" + "    line=%s",
                                                        rid, score, i, line.substring(0, 60)));
                                            output.append(line);
                                            logger.log(
                                                    Level.DEBUG, String.format("length of line is %d", line.length()));
                                            logger.log(Level.DEBUG, String.format("line is %s.", line));
                                            output.append('\n');
                                        }
                                    } else {
                                        logger.log(Level.DEBUG, " Skipping rest");
                                        break;
                                    }
                                    ++i;
                                }
                                ps_write_to_hdfs(rid, output.toString());
                            }

                            logger.log(Level.DEBUG, String.format("close %d", partitionId));
                            try {
                                if (false) fs.close();
                            } catch (IOException ioe) {
                                logger.log(Level.DEBUG, "Couldn't close HDFS filesystem");
                            }

                            return;
                        } // close

                        private void ps_write_to_hdfs(String rid, String output) {
                            try {
                                String tmpfile = String.format("/tmp/%s_hsp.json", rid);
                                FSDataOutputStream os = fs.create(new Path(tmpfile));
                                os.writeBytes(output);
                                os.close();

                                Path newFolderPath = new Path(hsp_result_dir);
                                if (!fs.exists(newFolderPath)) {
                                    logger.log(Level.DEBUG, "Creating HDFS dir " + hsp_result_dir);
                                    fs.mkdirs(newFolderPath);
                                }
                                String outfile = String.format("%s/%s.txt", hsp_result_dir, rid);
                                fs.delete(new Path(outfile), false);
                                // Rename in HDFS is supposed to be atomic
                                fs.rename(new Path(tmpfile), new Path(outfile));
                                logger.log(
                                        Level.INFO,
                                        String.format("Wrote %d bytes to HDFS %s", output.length(), outfile));

                            } catch (IOException ioe) {
                                logger.log(Level.DEBUG, "Couldn't write to HDFS");
                                logger.log(Level.DEBUG, ioe.toString());
                            }
                        } // ps_write_to_hdfs
                    } // ForeachWriter
        ) // foreach
            .outputMode("update");

        System.out.println("made  prelim_stream\n");
        return topn_dsw;
    }

    private static DataStreamWriter<Row> make_traceback_stream() {
        System.out.println("making traceback_stream");
        // BLAST_SETTINGS settings_closure = settings;
        Integer top_n = settings.top_n; // FIX: topn_n for traceback
        String jni_log_level = settings.jni_log_level;
        String db_location = "/tmp/blast/db"; // settings.db_location;

        StructType hsp_schema =
            StructType.fromDDL(
                    "max_score int, hsp_blob string, query_seq string, "
                    + "oid int, RID string, db string, partition_num int");

        DataStreamReader hsp_stream = sparksession.readStream();
        hsp_stream.format("json");
        hsp_stream.option("maxFilesPerTrigger", settings.max_backlog);
        // hsp_stream.option("multiLine", true);
        hsp_stream.option("mode", "FAILFAST");
        hsp_stream.option("includeTimestamp", true);
        hsp_stream.schema(hsp_schema);

        String hdfs_result_dir = settings.hdfs_result_dir;
        String hsp_result_dir = hdfs_result_dir + "/hsps";
        String gs_result_bucket = settings.gs_result_bucket;

        Dataset<Row> hsps = hsp_stream.json(hsp_result_dir);
        System.out.print("hsps schema is:");
        hsps.printSchema();
        // max_score, hsp_blob, query_seq, oid, RID, db, partition_num
        hsps.createOrReplaceTempView("hsps");

        Dataset<Row> parted =
            sparksession.sql("select * " + "from hsps " + "distribute by partition_num");
        parted.createOrReplaceTempView("parted");

        System.out.print("parted schema is ");
        parted.printSchema();

        Dataset<Row> tb_struct =
            sparksession.sql(
                    "select "
                    + "RID, db, cast(partition_num as int), query_seq, "
                    + "struct(oid, max_score, hsp_blob) as hsp "
                    + "from parted");
        System.out.print("tb_struct schema is ");
        tb_struct.printSchema();
        tb_struct.createOrReplaceTempView("tb_struct");
        Dataset<Row> tb_hspl =
            sparksession.sql(
                    "select "
                    + "RID, db, partition_num, query_seq, "
                    // + "to_json(collect_list(hsp)) as hspl "
                    + "collect_list(hsp) as hsp_collected "
                    + "from tb_struct "
                    + "group by RID, db, partition_num, query_seq "
                    + "distribute by partition_num");
        System.out.print("tb_hspl schema is ");
        tb_hspl.printSchema();

        //        Dataset<Row> traceback_results = tb_hspl;
        System.out.println("tb_hspl is: " + Arrays.toString(tb_hspl.columns()));
        // tb_hspl.explain(true);

        Dataset<Row> traceback_results =
            tb_hspl
            .flatMap( // FIX: Make a functor
                    (FlatMapFunction<Row, String>)
                    inrow -> {
                        Logger logger = LogManager.getLogger(BLAST_DRIVER.class);
                        logger.log(Level.INFO, "tb <row> is :" + inrow.mkString(":"));

                        String rid = inrow.getString(inrow.fieldIndex("RID"));
                        logger.log(Level.DEBUG, "Tracebacked RID " + rid);
                        String db = inrow.getString(inrow.fieldIndex("db"));
                        int partition_num = inrow.getInt(inrow.fieldIndex("partition_num"));
                        String query_seq = inrow.getString(inrow.fieldIndex("query_seq"));
                        logger.log(
                                Level.DEBUG,
                                String.format(
                                    "in tb flatmap rid=%s part=%d settings.db_location=%s",
                                    rid, partition_num, db_location));

                        List<Row> hsplist = inrow.getList(inrow.fieldIndex("hsp_collected"));
                        // logger.log(Level.INFO, "alhspl is " + alhspl.toString());
                        ArrayList<BLAST_HSP_LIST> hspal = new ArrayList<>(hsplist.size());

                        for (Row hsp : hsplist) {
                            logger.log(Level.DEBUG, "alhspl # " + hsp.mkString(":"));
                            StructType sc = hsp.schema();
                            logger.log(Level.DEBUG, "alhspl schema:" + sc.toString());
                            int oid = hsp.getInt(hsp.fieldIndex("oid"));
                            int max_score = hsp.getInt(hsp.fieldIndex("max_score"));
                            String hsp_blob = hsp.getString(hsp.fieldIndex("hsp_blob"));
                            logger.log(
                                    Level.DEBUG,
                                    String.format(
                                        "alhspl oid=%d max_score=%d blob=%d bytes",
                                        oid, max_score, hsp_blob.length()));
                            byte[] blob = Base64.getDecoder().decode(hsp_blob);

                            BLAST_HSP_LIST hspl = new BLAST_HSP_LIST(oid, max_score, blob);
                            hspal.add(hspl);
                        }

                        BLAST_REQUEST requestobj = new BLAST_REQUEST();
                        requestobj.id = rid;
                        requestobj.query_seq = query_seq;
                        requestobj.query_url = "";
                        requestobj.db = db;
                        requestobj.params = "blastn";
                        requestobj.program = "blastn";
                        requestobj.top_n = top_n;
                        BLAST_PARTITION partitionobj =
                            new BLAST_PARTITION(db_location, "nt_50M", partition_num, false);

                        BLAST_LIB blaster = new BLAST_LIB();

                        List<String> asns;
                        if (blaster != null) {
                            asns = new ArrayList<>();

                            BLAST_HSP_LIST[] hsparray = hspal.toArray(new BLAST_HSP_LIST[hspal.size()]);
                            logger.log(
                                    Level.INFO,
                                    String.format(
                                        " spark calling traceback with %d HSP_LISTS", hsparray.length));
                            BLAST_TB_LIST[] tb_res =
                                blaster.jni_traceback(
                                        hsparray, partitionobj, requestobj, jni_log_level);
                            logger.log(
                                    Level.INFO,
                                    String.format(" traceback returned %d blobs to Spark", tb_res.length));

                            for (BLAST_TB_LIST tb : tb_res) {
                                JSONObject json = new JSONObject();
                                json.put("RID", rid);
                                json.put("oid", tb.oid);
                                json.put("evalue", tb.evalue);
                                byte[] encoded = Base64.getEncoder().encode(tb.asn1_blob);
                                String b64blob = new String(encoded, StandardCharsets.UTF_8);
                                json.put("asn1_blob", b64blob);

                                asns.add(json.toString());
                            }
                        } else {
                            logger.log(Level.ERROR, "NULL blaster library");
                            asns = new ArrayList<>();
                            asns.add("null blaster ");
                        }
                        return asns.iterator();
                    },
                          Encoders.STRING())
                              .toDF("asns")
                              .repartition(1); // FIX

        traceback_results.explain();

        DataStreamWriter<Row> tb_dsw = traceback_results.writeStream();

        DataStreamWriter<Row> out_dsw =
            tb_dsw
            .foreach( // FIX: Make a separate function
                    new ForeachWriter<Row>() {
                        private long partitionId;
                        private Logger logger;
                        HashMap<String, TreeMap<Double, ArrayList<String>>> score_map;

                        @Override
                        public boolean open(long partitionId, long version) {
                            score_map = new HashMap<>();

                            this.partitionId = partitionId;
                            logger = LogManager.getLogger(BLAST_DRIVER.class);
                            logger.log(Level.DEBUG, String.format("tb open %d %d", partitionId, version));
                            if (partitionId != 0)
                                logger.log(
                                        Level.DEBUG, String.format(" *** not partition 0 %d ??? ", partitionId));
                            return true;
                        } // open

                        @Override
                        public void process(Row value) {
                            logger.log(Level.DEBUG, String.format(" in tb process %d", partitionId));
                            logger.log(Level.DEBUG, " tb process " + value.mkString(":"));
                            String line = value.getString(0);
                            JSONObject json = new JSONObject(line);
                            String rid = json.getString("RID");
                            double evalue = json.getDouble("evalue");
                            // int oid=json.getInt("oid");
                            String asn1_blob = json.getString("asn1_blob");

                            if (!score_map.containsKey(rid)) {
                                TreeMap<Double, ArrayList<String>> tm =
                                    new TreeMap<Double, ArrayList<String>>(Collections.reverseOrder());
                                score_map.put(rid, tm);
                            }

                            TreeMap<Double, ArrayList<String>> tm = score_map.get(rid);

                            // FIX optimize: early cutoff if tm.size>topn
                            if (!tm.containsKey(evalue)) {
                                ArrayList<String> al = new ArrayList<String>();
                                tm.put(evalue, al);
                            }
                            ArrayList<String> al = tm.get(evalue);

                            al.add(asn1_blob);
                            tm.put(evalue, al);
                            score_map.put(rid, tm);
                        }

                        @Override
                        public void close(Throwable errorOrNull) {
                            logger.log(Level.DEBUG, "tb close results:");
                            logger.log(Level.DEBUG, "---------------");

                            ByteArrayOutputStream bytes = new ByteArrayOutputStream();

                            byte[] seq_annot_prefix = {
                                (byte) 0x30,
                                (byte) 0x80,
                                (byte) 0xa4,
                                (byte) 0x80,
                                (byte) 0xa1,
                                (byte) 0x80,
                                (byte) 0x31,
                                (byte) 0x80
                            };
                            byte[] seq_annot_suffix = {0, 0, 0, 0, 0, 0, 0, 0};
                            for (String rid : score_map.keySet()) {
                                TreeMap<Double, ArrayList<String>> tm = score_map.get(rid);

                                bytes.write(seq_annot_prefix, 0, seq_annot_prefix.length);

                                int i = 0;
                                for (Double score : tm.keySet()) {
                                    if (i < top_n) {
                                        ArrayList<String> al = tm.get(score);
                                        for (String line : al) {
                                            byte[] blob = Base64.getDecoder().decode(line);
                                            bytes.write(blob, 0, blob.length);
                                        }
                                    } else {
                                        logger.log(Level.DEBUG, " Skipping rest");
                                        break;
                                    }
                                    ++i;
                                }
                                bytes.write(seq_annot_suffix, 0, seq_annot_suffix.length);

                                tb_write_to_hdfs(rid, bytes.toByteArray());
                            }

                            logger.log(Level.DEBUG, String.format("close %d", partitionId));

                            return;
                        } // close

                        private void tb_write_to_hdfs(String rid, byte[] output) {
                            try {
                                String outfile = "gs://" + gs_result_bucket + "/output/" + rid + ".asn1";
                                Configuration conf = new Configuration();
                                Path path = new Path(outfile);
                                FileSystem fs = FileSystem.get(path.toUri(), conf);

                                FSDataOutputStream os = fs.create(path);
                                os.write(output, 0, output.length);
                                os.close();
                                logger.log(
                                        Level.INFO,
                                        String.format("Wrote %d bytes to %s", output.length, outfile));

                            } catch (IOException ioe) {
                                logger.log(Level.DEBUG, "Couldn't write to HDFS");
                                logger.log(Level.DEBUG, ioe.toString());
                            }
                        } // tb_write_to_hdfs
                    } // ForeachWriter
        ) // foreach
            .outputMode("update"); // Must be complete or update for aggregations

        System.out.println("made  traceback_stream\n");
        return out_dsw;
    }

    private static boolean run_streams(
            DataStreamWriter<Row> prelim_dsw, DataStreamWriter traceback_dsw) {
        System.out.println("starting streams...");
        //  StreamingQuery prelim_results = prelim_dsw.outputMode("append").format("console").start();
        try {
            StreamingQuery presults = prelim_dsw.start();
            // traceback_dsw.format("console").option("truncate", false).start();
            StreamingQuery tresults = traceback_dsw.start();

            for (int i = 0; i < 30; ++i) {
                System.out.println("\nstreams running...\n");
                Thread.sleep(30000);
                System.out.println(presults.lastProgress());
                System.out.println(presults.status());
                System.out.println(tresults.lastProgress());
                System.out.println(tresults.status());
            }
        } catch (Exception e) {
            System.out.println("Spark exception: " + e);
            return false;
        }
        System.out.println("That is enough for now");

        return true;
            }

    private static void shutdown() {
        javasparkcontext.stop();
        sparksession.stop();
    }

    public static void main(String[] args) throws Exception {
        boolean result;
        result = init(args);
        if (!result) {
            shutdown();
            System.exit(1);
        }

        if (!make_partitions()) {
            shutdown();
            System.exit(2);
        }

        DataStreamWriter<Row> prelim_stream = make_prelim_stream();
        DataStreamWriter<Row> traceback_stream = make_traceback_stream();
        result = run_streams(prelim_stream, traceback_stream);
        if (!result) {
            shutdown();
            System.exit(3);
        }
    }
}
