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
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public final class BLAST_DRIVER implements Serializable {
  private static SparkSession sparksession;
  // Only one spark context allowed per JVM
  private static JavaSparkContext javasparkcontext;
  private final String db_location = "/tmp/blast/db";
  private BLAST_SETTINGS settings;
  // private int max_partitions = 0;
  private BLAST_DB_SETTINGS dbsettings;
  private Logger log;
  private String hsp_result_dir;

  public void BLAST_DRIVER() {}

  public boolean init(final String[] args) {
    if (args.length != 1) {
      System.out.println("settings json-file missing");
      return false;
    }
    log = LogManager.getLogger(BLAST_DRIVER.class);

    final String ini_path = args[0];

    final String appName = "experiment_dataset";

    final Tracer tracer = Tracing.getTracer();

    Span rootSpan = tracer.spanBuilderWithExplicitParent(appName, null).startSpan();
    rootSpan.addAnnotation("Annotation to the root Span before child is created.");
    Span childSpan =
        tracer.spanBuilderWithExplicitParent(appName + "_blaster", rootSpan).startSpan();
    childSpan.addAnnotation("Annotation to the child Span");
    childSpan.end();
    rootSpan.addAnnotation("Annotation to the root Span after child is ended.");
    rootSpan.end();

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

    dbsettings = settings.db_list;
    conf.set("spark.dynamicAllocation.enabled", Boolean.toString(settings.with_dyn_alloc));
    conf.set("spark.eventLog.enabled", "false");

    if (settings.num_executor_cores > 0)
      conf.set("spark.executor.cores", String.format("%d", settings.num_executor_cores));

    // These will appear in
    // executor:/var/log/hadoop-yarn/userlogs/applica*/container*/stdout
    // FIX: +UseParallelGC ? Increase G1GC latency?
    conf.set("spark.executor.extraJavaOptions", "-XX:+PrintCompilation -verbose:gc");

    if (settings.num_executors > 0)
      conf.set("spark.executor.instances", String.format("%d", settings.num_executors));
    if (!settings.executor_memory.isEmpty())
      conf.set("spark.executor.memory", settings.executor_memory);

    conf.set("spark.locality.wait", settings.locality_wait);

    // -> process, node, rack, any
    if (settings.scheduler_fair)
      conf.set(
          "spark.scheduler.mode",
          "FAIR"); // FIX, need fairscheduler.xml, see /etc/spark/conf.dist/fair_scheduler.xml
    conf.set("spark.shuffle.reduceLocality.enabled", "false");

    // conf.set("spark.sql.shuffle.partitions", Integer.toString(max_partitions));
    // conf.set("spark.sql.streaming.schemaInference", "true");
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    conf.set("spark.sql.warehouse.dir", warehouseLocation);

    conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
    conf.set("spark.streaming.receiver.maxRate", String.format("%d", settings.max_backlog));

    conf.set("spark.ui.enabled", "true");

    System.out.println("Default parallelism is" + conf.get("spark.default.parallelism"));
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
    files_to_transfer.add("dbs.json");
    for (String a_file : files_to_transfer) {
      javasparkcontext.addFile(a_file);
    }

    hsp_result_dir = settings.hdfs_result_dir + "/hsps";

    return true;
  }

  private Dataset<PART> make_partitions() {

    List<PART> lparts = new ArrayList<>();

    BLAST_DB_SETTING nt = dbsettings.get("nt");
    for (int i = 0; i != nt.num_partitions; ++i) {
      PART part = new PART();
      part.setPart_num(i);
      part.setDb_select("nt");
      lparts.add(part);
    }

    BLAST_DB_SETTING nr = dbsettings.get("nr");
    for (int i = 0; i != nt.num_partitions; ++i) {
      PART part = new PART();
      part.setPart_num(i);
      part.setDb_select("nr");
      lparts.add(part);
    }

    System.out.println(String.format("blast_partitions has %d entries", lparts.size()));

    Dataset<PART> blast_partitions = sparksession.createDataset(lparts, Encoders.bean(PART.class));
    Dataset<PART> blast_partitions_part =
        blast_partitions
            .repartition(settings.num_executors, blast_partitions.col("db_select"))
            .persist(StorageLevel.MEMORY_AND_DISK());

    blast_partitions_part.show();
    // blast_partitions.createOrReplaceTempView("blast_partitions");

    return blast_partitions_part;
  }

  MapFunction<String, BLAST_QUERY> jsontoqueryfunc =
      new MapFunction<String, BLAST_QUERY>() {
        @Override
        public BLAST_QUERY call(String json) {
          log.log(Level.INFO, "input json was:" + json);

          BLAST_QUERY query = new BLAST_QUERY(json);
          log.log(Level.INFO, "     parsed is:" + query.toString());

          return query;
        }
      };

  private Dataset<BLAST_QUERY> json_parser(Dataset<String> queries) {
    Dataset<BLAST_QUERY> parsed = queries.map(jsontoqueryfunc, Encoders.bean(BLAST_QUERY.class));

    return parsed;
  }

  private boolean copyfile(String src, String dest) {

    final File donefile = new File(dest + ".done");
    int loops = 0;
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
            Level.ERROR,
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
    log.log(Level.INFO, "db_selector is " + db_selector);
    // log.log(Level.INFO, "dbsettings is " + dbsettings.toString());

    BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
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

  FlatMapFunction<BLAST_QUERY, BLAST_QUERY> prelim_search_func =
      new FlatMapFunction<BLAST_QUERY, BLAST_QUERY>() {
        @Override
        public Iterator<BLAST_QUERY> call(BLAST_QUERY query) {
          String jni_log_level = settings.jni_log_level;
          log.log(Level.INFO, "prelim_search");

          String db_selector = query.getDb_selector();
          BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
          String pattern = dbs.pattern; // nt_50M
          int partition_num = query.getPartition_num();

          BLAST_REQUEST requestobj = new BLAST_REQUEST();
          requestobj.id = query.getRid();
          ;
          requestobj.query_seq = query.getQuery_seq();
          requestobj.query_url = query.getQuery_url();
          requestobj.db = db_selector;
          requestobj.program = query.getProgram();
          requestobj.params = query.getBlast_params();
          requestobj.top_n = query.getTop_N_prelim();
          BLAST_PARTITION partitionobj =
              new BLAST_PARTITION(db_location, pattern, partition_num, true);
          log.log(Level.INFO, "PARTOBJ is " + partitionobj.toString());

          preload(db_selector, partition_num);

          BLAST_LIB blaster = new BLAST_LIB();
          if (blaster != null) log.log(Level.ERROR, "NULL blaster library");

          ArrayList<BLAST_QUERY> parts = new ArrayList<BLAST_QUERY>();
          try {
            BLAST_HSP_LIST[] search_res =
                blaster.jni_prelim_search(partitionobj, requestobj, jni_log_level);
            if (search_res.length > 0)
              log.log(
                  Level.INFO,
                  String.format(" prelim returned %d hsps to Spark", search_res.length));

            for (BLAST_HSP_LIST S : search_res) {
              BLAST_QUERY result = new BLAST_QUERY(query);
              result.setHsp_blob(S.hsp_blob);
              result.setOid(S.oid);
              result.setMax_score(S.max_score);
              parts.add(result);
            }
          } catch (Exception e) {
            log.log(Level.ERROR, "jni_prelim_search threw exception" + e.toString());
          }

          return parts.iterator();
        }
      };

  FlatMapFunction<BLAST_QUERY, BLAST_QUERY> traceback_func =
      new FlatMapFunction<BLAST_QUERY, BLAST_QUERY>() {
        @Override
        public Iterator<BLAST_QUERY> call(BLAST_QUERY query) {
          String jni_log_level = settings.jni_log_level;
          log.log(Level.INFO, "traceback");

          String db_selector = query.getDb_selector();
          BLAST_DB_SETTING dbs = dbsettings.get(db_selector);
          String pattern = dbs.pattern; // nt_50M
          int partition_num = query.getPartition_num();

          BLAST_REQUEST requestobj = new BLAST_REQUEST();
          requestobj.id = query.getRid();
          ;
          requestobj.query_seq = query.getQuery_seq();
          requestobj.query_url = query.getQuery_url();
          requestobj.db = db_selector;
          requestobj.program = query.getProgram();
          requestobj.params = query.getBlast_params();
          requestobj.top_n = query.getTop_N_traceback();
          BLAST_PARTITION partitionobj =
              new BLAST_PARTITION(db_location, pattern, partition_num, true);
          log.log(Level.INFO, "PARTOBJ is " + partitionobj.toString());

          preload(db_selector, partition_num);

          // FIX
          BLAST_HSP_LIST hspl =
              new BLAST_HSP_LIST(query.getOid(), query.getMax_score(), query.getHsp_blob());
          BLAST_HSP_LIST[] hsparray = new BLAST_HSP_LIST[1];
          hsparray[0] = hspl;

          BLAST_LIB blaster = new BLAST_LIB();
          if (blaster != null) log.log(Level.ERROR, "NULL blaster library");

          ArrayList<BLAST_QUERY> parts = new ArrayList<BLAST_QUERY>();
          try {
            BLAST_TB_LIST[] tb_res =
                blaster.jni_traceback(hsparray, partitionobj, requestobj, jni_log_level);
            log.log(
                Level.INFO, String.format(" traceback returned %d blobs to Spark", tb_res.length));

            for (BLAST_TB_LIST tb : tb_res) {
              BLAST_QUERY tbquery = new BLAST_QUERY(query);
              tbquery.setOid(tb.oid);
              tbquery.setEvalue(tb.evalue);
              tbquery.setAsn1_blob(tb.asn1_blob);

              parts.add(tbquery);
            }
          } catch (Exception e) {
            log.log(Level.ERROR, "jni_traceback threw " + e.toString());
          }

          return parts.iterator();
        }
      };

  private Dataset<BLAST_QUERY> prelim_results(Dataset<BLAST_QUERY> queries) {

    Dataset<BLAST_QUERY> prelim_search_results =
        queries
            .flatMap(prelim_search_func, Encoders.bean(BLAST_QUERY.class))
            .repartition(queries.col("RID"));
    // Don't use coalesce here, it'll group previous work onto one worker

    return prelim_search_results;
  }

  private Dataset<BLAST_QUERY> traceback_results(Dataset<BLAST_QUERY> hsps) {

    Dataset<BLAST_QUERY> traceback_results =
        hsps.flatMap(traceback_func, Encoders.bean(BLAST_QUERY.class))
            .repartition(hsps.col("RID")); // Coalesce here?

    return traceback_results;
  }

  private void write_to_hdfs(String dir, String rid, byte[] output) {
    String outfile = String.format("%s/%s", dir, rid);
    try {
      if (dir.toLowerCase().startsWith("gs://")) {
        Path path = new Path(outfile);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream os = fs.create(new Path(outfile));
        os.write(output, 0, output.length);
        os.close();
      } else {
        String tmpfile = String.format("/tmp/%s", rid);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path newFolderPath = new Path(dir);
        if (!fs.exists(newFolderPath)) {
          log.log(Level.INFO, "Creating HDFS dir " + dir);
          fs.mkdirs(newFolderPath);
        }

        // Rename in HDFS is supposed to be atomic
        fs.rename(new Path(tmpfile), new Path(outfile));
        fs.delete(new Path(tmpfile));
      }
    } catch (IOException e) {
      log.log(Level.ERROR, "Couldn't write" + outfile + "to GS/HDFS");
      log.log(Level.ERROR, e.toString());
      System.err.println(e);
      return;
    }
    log.log(Level.INFO, String.format("Wrote %d bytes to %s", output.length, outfile));
  } // write_to_hdfs

  private DataStreamWriter<BLAST_QUERY> prelim_topn_dsw(DataStreamWriter<BLAST_QUERY> prelim_dsw) {
    DataStreamWriter<BLAST_QUERY> topn_dsw =
        prelim_dsw
            .foreach(
                // FIX: Make a separate class
                new ForeachWriter<BLAST_QUERY>() {
                  private long partitionId;
                  private int recordcount = 0;
                  private Logger logger;
                  private BLAST_TOPN topn;
                  // FIX map of RID->topn_N_prelims

                  @Override
                  public boolean open(long partitionId, long version) {
                    topn = new BLAST_TOPN();
                    this.partitionId = partitionId;

                    log.log(Level.INFO, String.format("topn_dsw open %d %d", partitionId, version));
                    return true;
                  } // open

                  @Override
                  public void process(BLAST_QUERY result) {
                    ++recordcount;
                    log.log(Level.INFO, String.format(" topn_dsw in process %d", partitionId));
                    log.log(Level.INFO, "topn_dsw " + result.toString());
                    double d = result.getMax_score();
                    topn.add(result.getRid(), d, result);
                  }

                  @Override
                  public void close(Throwable errorOrNull) {
                    log.log(Level.INFO, "topn_dsw close results:");
                    log.log(Level.INFO, "---------------");
                    log.log(Level.INFO, String.format(" topn_dsw saw %d records", recordcount));

                    ArrayList<ArrayList<BLAST_QUERY>> results = topn.results();

                    log.log(
                        Level.INFO,
                        String.format(
                            "topn_dsw close partition %d had %d RIDs",
                            partitionId, results.size()));
                    for (ArrayList<BLAST_QUERY> r : results) {
                      for (BLAST_QUERY q : r) {
                        log.log(Level.INFO, "topn_dsw result: " + r.toString());
                        String rid = q.getRid();
                        // FIX, combine

                        write_to_hdfs(
                            settings.hdfs_result_dir,
                            rid,
                            r.toString().getBytes(StandardCharsets.UTF_8));
                      }
                    }
                    return;
                  } // close
                } // ForeachWriter
                ) // foreach
            .outputMode("update");

    return topn_dsw;
  } // prelim_topn_dsw

  private DataStreamWriter<BLAST_QUERY> traceback_topn_dsw(DataStreamWriter<BLAST_QUERY> tb_dsw) {
    DataStreamWriter<BLAST_QUERY> topn_dsw =
        tb_dsw
            .foreach(
                // FIX: Make a separate class
                new ForeachWriter<BLAST_QUERY>() {
                  private long partitionId;
                  private int recordcount = 0;
                  private FileSystem fs;
                  private Logger logger;
                  private BLAST_TOPN topn;

                  @Override
                  public boolean open(long partitionId, long version) {
                    topn = new BLAST_TOPN();
                    this.partitionId = partitionId;
                    try {
                      Configuration conf = new Configuration();
                      fs = FileSystem.get(conf);
                    } catch (IOException e) {
                      log.log(Level.ERROR, e.toString());
                      System.err.println(e);
                      return false;
                    }

                    log.log(
                        Level.INFO,
                        String.format("traceback_topn_dsw open %d %d", partitionId, version));
                    return true;
                  } // open

                  @Override
                  public void process(BLAST_QUERY result) {
                    ++recordcount;
                    log.log(
                        Level.INFO,
                        String.format(" traceback_topn_dsw in process %d", partitionId));
                    log.log(Level.INFO, "topn_dsw " + result.toString());
                    double d = result.getMax_score();
                    topn.add(result.getRid(), d, result);
                  }

                  @Override
                  public void close(Throwable errorOrNull) {
                    log.log(Level.INFO, "traceback topn_dsw close results:");
                    log.log(Level.INFO, "---------------");
                    log.log(Level.INFO, String.format(" topn_dsw saw %d records", recordcount));

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

                    ArrayList<ArrayList<BLAST_QUERY>> results = topn.results();
                    for (ArrayList<BLAST_QUERY> al : results) {
                      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                      bytes.write(seq_annot_prefix, 0, seq_annot_prefix.length);
                      String rid = "unknown";
                      for (BLAST_QUERY b : al) {
                        rid = b.getRid();
                        bytes.write(b.getAsn1_blob(), 0, b.getAsn1_blob().length);
                      }
                      bytes.write(seq_annot_suffix, 0, seq_annot_suffix.length);

                      write_to_hdfs(
                          "gs://" + settings.gs_result_bucket,
                          "/output/" + rid + ".asn1",
                          bytes.toByteArray());
                    }

                    log.log(Level.DEBUG, String.format("close %d", partitionId));

                    return;
                  } // close
                } // ForeachWriter
                ) // foreach
            .outputMode("update");

    return topn_dsw;
  }

  MapFunction<Tuple2<PART, BLAST_QUERY>, BLAST_QUERY> fixfunc =
      new MapFunction<Tuple2<PART, BLAST_QUERY>, BLAST_QUERY>() {
        @Override
        public BLAST_QUERY call(Tuple2<PART, BLAST_QUERY> tup) {
          PART p = tup._1();
          int part_num = p.getPart_num();

          BLAST_QUERY q = tup._2();
          q.setPartition_num(part_num);
          return q;
        }
      };

  private DataStreamWriter<BLAST_QUERY> make_prelim_stream(Dataset<PART> blast_partitions) {
    System.out.println("making prelim_stream");

    DataStreamReader query_stream = sparksession.readStream();
    query_stream.option("maxFilesPerTrigger", settings.max_backlog);
    // Each line is a new element
    Dataset<String> queries = query_stream.textFile(settings.hdfs_source_dir);

    Dataset<BLAST_QUERY> parsed = json_parser(queries);
    System.out.print("parsed schema is:");
    parsed.printSchema();

    Dataset<Tuple2<PART, BLAST_QUERY>> joined_tup =
        blast_partitions.joinWith(
            parsed, parsed.col("db_selector").equalTo(blast_partitions.col("db_select")));

    Dataset<BLAST_QUERY> joined = joined_tup.map(fixfunc, Encoders.bean(BLAST_QUERY.class));

    System.out.print("joined is:");
    joined.show();

    Dataset<BLAST_QUERY> results = prelim_results(joined);

    DataStreamWriter<BLAST_QUERY> prelim_dsw = results.writeStream();

    DataStreamWriter<BLAST_QUERY> topn_dsw = prelim_topn_dsw(prelim_dsw);

    System.out.println("made  prelim_stream\n");

    return topn_dsw;
  }

  private DataStreamWriter<BLAST_QUERY> make_traceback_stream(Dataset<PART> blast_partitions) {
    System.out.println("making traceback_stream");

    String hdfs_result_dir = settings.hdfs_result_dir;
    String hsp_result_dir = settings.hdfs_result_dir + "/hsps";

    DataStreamReader hsp_stream = sparksession.readStream();
    hsp_stream.option("maxFilesPerTrigger", settings.max_backlog);

    // Each line is a new element
    Dataset<String> parsed = hsp_stream.textFile(hsp_result_dir);
    Dataset<BLAST_QUERY> hsps = json_parser(parsed);
    System.out.print("hsps schema is:");
    hsps.printSchema();
    // hsps.createOrReplaceTempView("hsps");

    Dataset<Tuple2<PART, BLAST_QUERY>> joined_tup =
        blast_partitions.joinWith(
            hsps, hsps.col("db_selector").equalTo(blast_partitions.col("db_select")));

    Dataset<BLAST_QUERY> joined = joined_tup.map(fixfunc, Encoders.bean(BLAST_QUERY.class));

    Dataset<BLAST_QUERY> traceback_results = traceback_results(joined).repartition(1); // FIX

    // traceback_results.explain();

    DataStreamWriter<BLAST_QUERY> tb_dsw = traceback_results.writeStream();

    DataStreamWriter<BLAST_QUERY> out_dsw = traceback_topn_dsw(tb_dsw);

    System.out.println("made  traceback_stream\n");
    return out_dsw;
  }

  private boolean run_streams(
      DataStreamWriter<BLAST_QUERY> prelim_dsw, DataStreamWriter<BLAST_QUERY> traceback_dsw) {
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

  public static void main(String[] args) throws Exception {
    boolean result;

    BLAST_DRIVER driver = new BLAST_DRIVER();

    result = driver.init(args);
    if (!result) {
      driver.shutdown();
      System.exit(1);
    }

    Dataset<PART> blast_partitions = driver.make_partitions();

    DataStreamWriter<BLAST_QUERY> prelim_stream = driver.make_prelim_stream(blast_partitions);
    DataStreamWriter<BLAST_QUERY> traceback_stream = driver.make_traceback_stream(blast_partitions);

    result = driver.run_streams(prelim_stream, traceback_stream);
    if (!result) {
      driver.shutdown();
      System.exit(3);
    }
  }
}
