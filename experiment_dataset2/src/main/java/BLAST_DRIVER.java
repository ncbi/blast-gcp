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
 *===========================================================================
 *
 */

package gov.nih.nlm.ncbi.blastjni;

// import static org.apache.spark.sql.functions.*;

import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
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
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

public final class BLAST_DRIVER implements Serializable {
  private static SparkSession sparksession;
  // Only one spark context allowed per JVM
  private static JavaSparkContext javasparkcontext;
  private static final String db_location = "/tmp/blast/db";
  private BLAST_SETTINGS settings;
  // private int max_partitions = 0;
  private BLAST_DB_SETTINGS dbsettings;
  private transient Logger log; // Don't serialize
  private String hsp_result_dir;

  public boolean init(final String[] args) {
    if (args.length != 1) {
      System.out.println("settings json-file missing");
      return false;
    }
    log = LogManager.getLogger(BLAST_DRIVER.class);

    final String ini_path = args[0];

    final String appName = "experiment_dataset2";

    log.log(Level.INFO,"Starting " + appName);

    try {
      StackdriverTraceExporter.createAndRegister(
          StackdriverTraceConfiguration.builder()
              //              .setProjectId("ncbi-sandbox-blast")
              //              .setCredentials(new GoogleCredentials(new AccessToken(accessToken,
              // expirationTime)))
              .build());

      final Tracer tracer = Tracing.getTracer();
      System.out.println("Got tracer");

      System.out.println("Building span");
      Span rootSpan = tracer.spanBuilderWithExplicitParent(appName, null).startSpan();
      System.out.println("Adding annotation");
      rootSpan.addAnnotation("Annotation to the root Span before child is created.");
      System.out.println("Building child span");
      Span childSpan =
          tracer.spanBuilderWithExplicitParent(appName + "_blaster", rootSpan).startSpan();
      childSpan.addAnnotation("Annotation to the child Span");
      childSpan.end();
      rootSpan.addAnnotation("Annotation to the root Span after child is ended.");
      rootSpan.end();
    } catch (IOException e) {
      log.log(Level.ERROR, "Couldn't register stackdriver tracing: " + e);
    } catch (ServiceConfigurationError se) {
      log.log(Level.ERROR, "Couldn't register stackdriver tracing: " + se);
    }

    settings = BLAST_SETTINGS_READER.read_from_json(ini_path, appName);
    System.out.println(String.format("settings read from '%s'", ini_path));
    if (!settings.valid()) {
      System.out.println(settings.missing());
      return false;
    }

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

    dbsettings = settings.db_list;
    conf.set("spark.dynamicAllocation.enabled", Boolean.toString(settings.with_dyn_alloc));
    conf.set("spark.eventLog.enabled", "false");

    if (settings.num_executor_cores > 0) {
      conf.set("spark.executor.cores", String.format("%d", settings.num_executor_cores));
    }

    // These will appear in
    // executor:/var/log/hadoop-yarn/userlogs/applica*/container*/stdout
    // FIX: +UseParallelGC ? Increase G1GC latency?
    conf.set("spark.executor.extraJavaOptions", "-XX:+PrintCompilation -verbose:gc");

    if (settings.num_executors > 0) {
      conf.set("spark.executor.instances", String.format("%d", settings.num_executors));
    }
    if (!settings.executor_memory.isEmpty()) {
      conf.set("spark.executor.memory", settings.executor_memory);
    }

    conf.set("spark.locality.wait", settings.locality_wait);

    // -> process, node, rack, any
    if (settings.scheduler_fair) {
      conf.set(
          "spark.scheduler.mode",
          "FAIR"); // FIX, need fairscheduler.xml, see /etc/spark/conf.dist/fair_scheduler.xml
    }
    conf.set("spark.shuffle.reduceLocality.enabled", "false");

    // conf.set("spark.sql.shuffle.partitions", Integer.toString(max_partitions));
    // conf.set("spark.sql.streaming.schemaInference", "true");
    final String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    conf.set("spark.sql.warehouse.dir", warehouseLocation);

    conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
    conf.set("spark.streaming.receiver.maxRate", String.format("%d", settings.max_backlog));

    conf.set("spark.ui.enabled", "true");

    builder.config(conf);
    System.out.println("Configuration is");
    System.out.println("----------------");
    System.out.println("    " + conf.toDebugString().replace("\n", "\n    "));
    System.out.println();
    builder.enableHiveSupport();
    // System.out.println("Default parallelism is" + conf.get("spark.default.parallelism"));

    sparksession = builder.getOrCreate();
    javasparkcontext = new JavaSparkContext(sparksession.sparkContext());
    javasparkcontext.setLogLevel(settings.spark_log_level);

    // send the given files to all nodes
    List<String> files_to_transfer = new ArrayList<>();
    files_to_transfer.add("libblastjni.so");
    files_to_transfer.add("log4j.properties");
    files_to_transfer.add("dbs.json");
    for (String a_file : files_to_transfer) {
      javasparkcontext.addFile(a_file);
    }

    hsp_result_dir = settings.hdfs_result_dir + "/hsps";

    return true;
  }

  private Dataset<Row> make_partitions() {
    // db partition_num
    //    StructType parts_schema = StructType.fromDDL("db_selector string, partition_num int");
    //    Spark will force nullable for Strings
    StructType parts_schema =
        new StructType().add("db_selector", "string", false).add("partition_num", "int", false);

    ArrayList<Row> rows = new ArrayList<Row>();

    BLAST_DB_SETTING nt = dbsettings.get("nt");
    for (int i = 0; i < nt.num_partitions; i++) {
      Row r = RowFactory.create("nt", Integer.valueOf(i));
      rows.add(r);
    }

    BLAST_DB_SETTING nr = dbsettings.get("nr");
    for (int i = 0; i < nr.num_partitions; i++) {
      Row r = RowFactory.create("nr", Integer.valueOf(i));
      rows.add(r);
    }

    int num_blast_partitions=rows.size();
    System.out.println(String.format("blast_partitions has %d entries", num_blast_partitions));

    Dataset<Row> parts = sparksession.createDataFrame(rows, parts_schema);
    // If N rows are randomly hashed to M buckets, only about M*0.6 buckets
    // filled. So to increase potential parallelism, partition on db and
    // partition_num
    Dataset<Row> blast_partitions = parts
//        .repartition(parts.col("partition_num"))
        .repartition(num_blast_partitions)
        .persist(StorageLevel.MEMORY_AND_DISK());

    blast_partitions.printSchema();
    blast_partitions.show();
    //        blast_partitions.createOrReplaceTempView("blast_partitions");

    return blast_partitions;
  }

  MapFunction<String, Row> jsontoqueryfunc =
      new MapFunction<String, Row>() {
        @Override
        public Row call(final String json) {
          log = LogManager.getLogger(BLAST_DRIVER.class);
          log.log(Level.INFO, "input json was:" + json);

          BLAST_QUERY query = new BLAST_QUERY(json);
          log.log(Level.INFO, "     parsed is:" + query.toString());
          final String db_selector = query.db_selector;
          // int partition_num = query.partition_num;
          final String outjson = query.toJson();
          log.log(Level.INFO, "     output is:" + outjson);
          // return RowFactory.create(db_selector, partition_num, outjson);
          return RowFactory.create(db_selector, outjson);
        }
      };

  private Dataset<Row> json_parser(final Dataset<String> queries) {
    StructType parsed_schema =
        new StructType()
            .add("db_selector", "string", false)
            //            .add("partition_num", "int", false)
            .add("ser", "string", false);

    ExpressionEncoder<Row> encoder = RowEncoder.apply(parsed_schema);
    Dataset<Row> parsed = queries.map(jsontoqueryfunc, encoder);

    return parsed;
  }

  FlatMapFunction<Row, Row> prelim_search_func =
      new FlatMapFunction<Row, Row>() {
        @Override
        public Iterator<Row> call(final Row inrow) {
          final String jni_log_level = settings.jni_log_level;
          log = LogManager.getLogger(BLAST_DRIVER.class);
          log.log(Level.INFO, "prelim_search");

          final int partition_num = inrow.getInt(inrow.fieldIndex("partition_num"));
          final String ser = inrow.getString(inrow.fieldIndex("ser"));
          final BLAST_QUERY query = new BLAST_QUERY(ser);
          query.partition_num = partition_num;
          query.prelim_partition_num = partition_num;
          log.log(Level.INFO, String.format("partition_num is %d", partition_num));

          final String db_selector =
              query.db_selector; // or inrow.getString(inrow,fieldIndex("db"));

          final BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
          final String pattern = dbs.pattern; // nt_50M

          BLAST_REQUEST requestobj = new BLAST_REQUEST();
          requestobj.id = query.rid;
          requestobj.query_seq = query.query_seq;
          requestobj.query_url = query.query_url;
          requestobj.db = db_selector;
          requestobj.program = query.program;
          requestobj.params = query.blast_params;
          requestobj.top_n = query.top_n_prelim;
          BLAST_PARTITION partitionobj =
              new BLAST_PARTITION(db_location, pattern, partition_num, true);
          log.log(Level.INFO, "PARTOBJ is " + partitionobj.toString());

          preload(db_selector, partition_num);

          BLAST_LIB blaster = new BLAST_LIB();
          if (blaster == null) {
            log.log(Level.FATAL, "NULL blaster library in prelim_search_func");
          }

          ArrayList<Row> parts = new ArrayList<Row>();
          try {
            BLAST_HSP_LIST[] search_res =
                blaster.jni_prelim_search(partitionobj, requestobj, jni_log_level);
              final String RID = query.rid;
            if (search_res.length > 0) {
              log.log(
                  Level.INFO,
                  String.format("Note: prelim of %s returned %d hsps to Spark", 
                      RID,
                      search_res.length));

              // BLAST_QUERY res=new BLAST_QUERY(query);
              // res.setHspl(search_res);
              query.hspl = search_res;

              final String resser = query.toJson();
              log.log(Level.INFO, "resser for " + RID + " is " + resser);
              Row outrow = RowFactory.create(RID, resser);
              parts.add(outrow);
            }
          } catch (Exception e) {
            log.log(Level.ERROR, "jni_prelim_search threw exception" + e.toString());
          }

          log.log(
              Level.INFO,
              String.format("Note: prelim_search_func returning %d parts", parts.size()));
          return parts.iterator();
        }
      };

  private Dataset<Row> prelim_results(final Dataset<Row> joined) {
    StructType results_schema =
        new StructType().add("RID", "string", false).add("ser", "string", false);
    //        StructType.fromDDL("RID string, ser string");
    ExpressionEncoder<Row> encoder = RowEncoder.apply(results_schema);

    Dataset<Row> prelim_search_results = joined.flatMap(prelim_search_func, encoder);

    return prelim_search_results; // FIX.repartition(prelim_search_results.col("RID"));
    // Don't use coalesce here, it'll group previous work onto one worker
  }

  private DataStreamWriter<Row> prelim_topn_dsw(final DataStreamWriter<Row> prelim_dsw) {
    DataStreamWriter<Row> topn_dsw =
        prelim_dsw
            .foreach(
                // FIX: Make a separate class
                new ForeachWriter<Row>() {
                  private ArrayList<BLAST_QUERY> results;
                  private long partitionId;
                  private Logger log;
                  private BLAST_TOPN topn;

                  @Override
                  public boolean open(final long partitionId, final long version) {
                    log = LogManager.getLogger(BLAST_DRIVER.class);
                    results = new ArrayList<>();
                    topn = new BLAST_TOPN();
                    this.partitionId = partitionId;

                    log.log(Level.INFO, String.format("topn_dsw open %d %d", partitionId, version));
                    return true;
                  } // open

                  @Override
                  public void process(final Row inrow) {
                    log.log(Level.INFO, String.format(" topn_dsw in process %d", partitionId));
                    final String RID = inrow.getString(inrow.fieldIndex("RID"));
                    final String ser = inrow.getString(inrow.fieldIndex("ser"));
                    log.log(Level.INFO, String.format ("topn_dsw for %s",RID));
                    BLAST_QUERY result = new BLAST_QUERY(ser);

                    BLAST_HSP_LIST[] hspl = result.hspl;

                    for (BLAST_HSP_LIST hsp : hspl) {
                      topn.add(RID, (double) hsp.max_score, result.top_n_prelim);
                    }
                    results.add(result);
                  }

                  @Override
                  public void close(Throwable errorOrNull) {
                    log.log(Level.INFO, "topn_dsw close results:");

                    log.log(
                        Level.INFO,
                        String.format(
                            "Note: topn_dsw close partition %d had %d RIDs",
                            partitionId, results.size()));

                    HashMap<String, Double> tops = topn.results();
                    for (String RID : tops.keySet()) {
                      Double cutoff = tops.get(RID);

                      for (BLAST_QUERY result : results) {
                        if (!RID.equals(result.rid)) continue;

                        ArrayList<BLAST_HSP_LIST> hsplist =
                            new ArrayList<>(Arrays.asList(result.hspl));
                        ArrayList<BLAST_HSP_LIST> survive = new ArrayList<>();
                        for (BLAST_HSP_LIST hspl : hsplist) {
                          if (hspl.max_score <= cutoff) {
                            survive.add(hspl);
                          }
                        }
                        log.log(Level.INFO, String.format("Note: %d survived", survive.size()));
                        BLAST_HSP_LIST[] hspl = survive.toArray(new BLAST_HSP_LIST[0]);
                        result.hspl = hspl;

                        write_to_hdfs(
                            hsp_result_dir,
                            RID,
                            result.toString().getBytes(StandardCharsets.UTF_8));
                      }
                    }
                  } // close
                } // ForeachWriter
                ) // foreach
            .outputMode("update");

    return topn_dsw;
  } // prelim_topn_dsw

  FlatMapFunction<Row, Row> traceback_func =
      new FlatMapFunction<Row, Row>() {
        @Override
        public Iterator<Row> call(final Row inrow) {
          Logger log = LogManager.getLogger(BLAST_DRIVER.class);
          final String jni_log_level = settings.jni_log_level;
          log.log(Level.INFO, "traceback");

          final int partition_num = inrow.getInt(inrow.fieldIndex("partition_num"));
          final String ser = inrow.getString(inrow.fieldIndex("ser"));
          final BLAST_QUERY query = new BLAST_QUERY(ser);

          final String db_selector = query.db_selector;
          BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
          final String pattern = dbs.pattern; // nt_50M

          BLAST_REQUEST requestobj = new BLAST_REQUEST();
          requestobj.id = query.rid;
          requestobj.query_seq = query.query_seq;
          requestobj.query_url = query.query_url;
          requestobj.db = db_selector;
          requestobj.program = query.program;
          requestobj.params = query.blast_params;
          requestobj.top_n = query.top_n_traceback;
          BLAST_PARTITION partitionobj =
              new BLAST_PARTITION(db_location, pattern, partition_num, true);
          log.log(Level.INFO, "PARTOBJ is " + partitionobj.toString());

          preload(db_selector, partition_num);

          BLAST_HSP_LIST hsparray[] = query.hspl;

          BLAST_LIB blaster = new BLAST_LIB();
          if (blaster == null) {
            log.log(Level.FATAL, "NULL blaster library in traceback_func");
          }

          ArrayList<Row> parts = new ArrayList<Row>();
          try {
            BLAST_TB_LIST[] tb_res =
                blaster.jni_traceback(hsparray, partitionobj, requestobj, jni_log_level);
            log.log(
                Level.INFO, String.format(" traceback returned %d blobs to Spark", tb_res.length));

            final String RID = query.rid;

            query.tbl = tb_res;
            final String resser = query.toJson();
            Row outrow = RowFactory.create(RID, resser);
            parts.add(outrow);
          } catch (Exception e) {
            log.log(Level.ERROR, "jni_traceback threw " + e.toString());
          }

          log.log(
              Level.INFO, String.format("Note: traceback_func returning %d parts", parts.size()));

          return parts.iterator();
        }
      };

  private Dataset<Row> traceback_results(final Dataset<Row> joined) {
    StructType results_schema =
        new StructType().add("RID", "string", false).add("ser", "string", false);
    // StructType.fromDDL("RID string, ser string");
    ExpressionEncoder<Row> encoder = RowEncoder.apply(results_schema);

    Dataset<Row> traceback_results = joined.flatMap(traceback_func, encoder);

    return traceback_results.repartition(traceback_results.col("RID"));
  }

  private DataStreamWriter<Row> traceback_topn_dsw(final DataStreamWriter<Row> tb_dsw) {
    DataStreamWriter<Row> topn_dsw =
        tb_dsw
            .foreach(
                new ForeachWriter<Row>() {
                  private ArrayList<BLAST_QUERY> results;
                  private long partitionId;
                  private FileSystem fs;
                  private Logger log;
                  private BLAST_TOPN topn;

                  @Override
                  public boolean open(long partitionId, long version) {
                    log = LogManager.getLogger(BLAST_DRIVER.class);
                    results = new ArrayList<>();
                    topn = new BLAST_TOPN();
                    this.partitionId = partitionId;
                    return true;
                  } // open

                  @Override
                  public void process(Row inrow) {
                    final String RID = inrow.getString(inrow.fieldIndex("RID"));
                    final String ser = inrow.getString(inrow.fieldIndex("ser"));
                    BLAST_QUERY result = new BLAST_QUERY(ser);

                    BLAST_TB_LIST[] tbl = result.tbl;
                    for (BLAST_TB_LIST tb : tbl) {
                      topn.add(RID, tb.evalue, result.top_n_traceback);
                    }

                    results.add(result);
                  } // process

                  @Override
                  public void close(Throwable errorOrNull) {
                    final byte[] seq_annot_prefix = {
                      (byte) 0x30,
                      (byte) 0x80,
                      (byte) 0xa4,
                      (byte) 0x80,
                      (byte) 0xa1,
                      (byte) 0x80,
                      (byte) 0x31,
                      (byte) 0x80
                    };
                    final byte[] seq_annot_suffix = {0, 0, 0, 0, 0, 0, 0, 0};

                    log.log(Level.INFO, "traceback topn_dsw close results:");
                    log.log(
                        Level.INFO,
                        String.format(" Note: topn_dsw saw %d records", results.size()));

                    HashMap<String, Double> tops = topn.results();
                    for (String RID : tops.keySet()) {
                      Double cutoff = tops.get(RID);

                      for (BLAST_QUERY result : results) {
                        if (!RID.equals(result.rid)) continue;

                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        bytes.write(seq_annot_prefix, 0, seq_annot_prefix.length);
                        ArrayList<BLAST_TB_LIST> tblist =
                            new ArrayList<>(Arrays.asList(result.tbl));
                        // ArrayList<BLAST_TB_LIST> survive=new ArrayList<>();
                        for (BLAST_TB_LIST tbl : tblist) {
                          if (tbl.evalue <= cutoff) {
                            bytes.write(tbl.asn1_blob, 0, tbl.asn1_blob.length);
                          }
                        }
                        //                                BLAST_TB_LIST[] = survive.toArray(new
                        // BLAST_TB_LIST[survive.size()]);
                        // result.settbl(survive);

                        bytes.write(seq_annot_suffix, 0, seq_annot_suffix.length);

                        write_to_hdfs(
                            "gs://" + settings.gs_result_bucket,
                            "/output/" + RID + ".asn1",
                            bytes.toByteArray());
                      }

                      log.log(Level.DEBUG, String.format("close %d", partitionId));
                    }

                    return;
                  } // close
                } // ForeachWriter
                ) // foreach
            .outputMode("update");

    return topn_dsw;
  }

  private void write_to_hdfs(String dir, String RID, final byte[] output) {
    final String outfile = String.format("%s/%s", dir, RID);
    try {
      Logger log = LogManager.getLogger(BLAST_DRIVER.class);
      if (dir.toLowerCase().startsWith("gs://")) {
        Path path = new Path(outfile);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream os = fs.create(new Path(outfile));
        os.write(output, 0, output.length);
        os.close();
      } else {
        final String tmpfile = String.format("/tmp/%s", RID);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path newFolderPath = new Path(dir);
        if (!fs.exists(newFolderPath)) {
          log.log(Level.INFO, "Creating HDFS dir " + dir);
          fs.mkdirs(newFolderPath);
        }

        // Rename in HDFS is supposed to be atomic
        fs.rename(new Path(tmpfile), new Path(outfile));
        fs.delete(new Path(tmpfile), false);
      }
    } catch (Exception e) {
      log.log(Level.ERROR, "Couldn't write" + outfile + "to GS/HDFS");
      log.log(Level.ERROR, e.toString());
      System.err.println(e);
      return;
    }
    log.log(Level.INFO, String.format("Note: Wrote %d bytes to %s", output.length, outfile));
  } // write_to_hdfs

  private boolean copyfile(String src, String dest) {
    Logger log = LogManager.getLogger(BLAST_DRIVER.class);

    final File donefile = new File(dest + ".done");
    if (donefile.exists()) {
      log.log(Level.INFO, "Preloaded already: " + dest);
      return true;
    }

    int retries = 0;
    int backoff = 500;
    final String lockname = dest + ".lock";
    File lockfile = new File(lockname);
    FileLock lock = null;
    FileChannel fileChannel = null;
    while (!donefile.exists()) {
      ++retries;
      try {
        if (!lockfile.exists())
          if (!lockfile.createNewFile()) log.log(Level.ERROR, lockname + " already exists?");

        java.nio.file.Path lockpath = Paths.get(lockname);
        fileChannel =
            FileChannel.open(
                lockpath, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        lock = fileChannel.lock();
        if (donefile.exists()) continue;

        // Lock succeeded, this thread may download
        log.log(Level.INFO, String.format("Preloading (attempt #%d) %s -> %s", retries, src, dest));
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path srcpath = new org.apache.hadoop.fs.Path(src);
        FileSystem fs = FileSystem.get(srcpath.toUri(), conf);
        Path dstpath = new org.apache.hadoop.fs.Path(dest);
        fs.copyToLocalFile(false, srcpath, dstpath, true);
        fs.close();
        if (new File(dest).length() != 0) {
          if (donefile.createNewFile()) {
            log.log(Level.INFO, "Preloaded " + src + " -> " + dest);
            continue;
          }
        } else {
          log.log(Level.ERROR, "Empty file " + dest);
        }
      } catch (Exception e) {
        log.log(
            Level.WARN,
            String.format(
                "Couldn't load (attempt #%d) %s from GS:// : %s", retries, src, e.toString()));
        try {
          if (lock != null) lock.release();
        } catch (Exception e2) {
        }
      }
      try {
        Thread.sleep(backoff);
        backoff *= 2;
      } catch (Exception e) {
      }
    } // !donefile.exists
    try {
      if (lock != null) lock.release();
      if (fileChannel != null) fileChannel.close();
      lockfile.delete();
    } catch (Exception e) {
      log.log(Level.ERROR, "Couldn't delete/unlock: " + e.toString());
    }
    return true;
  }

  private void preload(String db_selector, int partition_num) {
    Logger log = LogManager.getLogger(BLAST_DRIVER.class);
    log.log(Level.INFO, "db_selector is " + db_selector);
    // log.log(Level.INFO, "dbsettings is " + dbsettings.toString());

    final BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
    final String bucket = "gs://" + dbs.bucket;
    final String pattern = dbs.pattern; // nt_50M

    for (String ext : dbs.extensions) {
      final String src = String.format("%s/%s.%02d.%s", bucket, pattern, partition_num, ext);
      final String dest = String.format("%s/%s.%02d.%s", db_location, pattern, partition_num, ext);

      File dbdir = new File(db_location);
      if (!dbdir.exists()) dbdir.mkdirs();
      copyfile(src, dest);
    } // extensions
  } // preload

  private DataStreamWriter<Row> make_prelim_stream(final Dataset<Row> blast_partitions) {
    System.out.println("making prelim_stream");

    DataStreamReader query_stream = sparksession.readStream();
    query_stream.option("maxFilesPerTrigger", settings.max_backlog);
    // Each line is a new element
    Dataset<String> queries = query_stream.textFile(settings.hdfs_source_dir);

    Dataset<Row> parsed = json_parser(queries);
    System.out.print("parsed schema is:");
    parsed.printSchema();

    // Dataset<Row> joined = blast_partitions.join(parsed, "db_selector");
    Dataset<Row> joined = parsed.join(blast_partitions, "db_selector");
    System.out.print("joined schema is:");
    joined.printSchema();
    joined.explain(true);

    Dataset<Row> results = prelim_results(joined);
    System.out.print("results schema is:");
    results.printSchema();
    results.explain(true);

    DataStreamWriter<Row> prelim_dsw = results.writeStream();

    DataStreamWriter<Row> topn_dsw = prelim_topn_dsw(prelim_dsw);

    System.out.println("made  prelim_stream\n");

    return topn_dsw;
  }

  private DataStreamWriter<Row> make_traceback_stream(final Dataset<Row> blast_partitions) {
    System.out.println("making traceback_stream");

    DataStreamReader hsp_stream = sparksession.readStream();
    hsp_stream.option("maxFilesPerTrigger", settings.max_backlog);

    // Each line is a new element
    Dataset<String> queries = hsp_stream.textFile(hsp_result_dir);

    Dataset<Row> parsed = json_parser(queries);

    Dataset<Row> joined = parsed;
    /*
            parsed.join(
                blast_partitions,
                parsed
                    .col("db_selector")
                    .equalTo(blast_partitions.col("db_selector"))
                    .and(parsed.col("partition_num").equalTo(blast_partitions.col("partition_num"))));
    */

    Dataset<Row> traceback_results = traceback_results(joined);

    DataStreamWriter<Row> tb_dsw = traceback_results.writeStream();

    DataStreamWriter<Row> out_dsw = traceback_topn_dsw(tb_dsw);

    System.out.println("made  traceback_stream\n");
    return out_dsw;
  }

  private boolean run_streams(
      DataStreamWriter<Row> prelim_dsw, DataStreamWriter<Row> traceback_dsw) {
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
      System.err.println("Spark exception: " + e);
      return false;
    }
    System.out.println("That is enough for now");

    return true;
  }

  private void shutdown() {
    javasparkcontext.stop();
    sparksession.stop();
  }

  public static void main(final String[] args) throws Exception {
    boolean result;

    BLAST_DRIVER driver = new BLAST_DRIVER();

    result = driver.init(args);
    if (!result) {
      driver.shutdown();
      System.exit(1);
    }

    Dataset<Row> blast_partitions = driver.make_partitions();

    DataStreamWriter<Row> prelim_stream = driver.make_prelim_stream(blast_partitions);
    DataStreamWriter<Row> traceback_stream = driver.make_traceback_stream(blast_partitions);

    result = driver.run_streams(prelim_stream, traceback_stream);
    if (!result) {
      driver.shutdown();
      System.exit(3);
    }
  }
}
