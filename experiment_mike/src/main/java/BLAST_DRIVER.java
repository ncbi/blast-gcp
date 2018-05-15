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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

public final class BLAST_DRIVER {
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

    conf.set("spark.dynamicAllocation.enabled", Boolean.toString(settings.with_dyn_alloc));
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
    conf.set("spark.streaming.receiver.maxRate", String.format("%d", settings.receiver_max_rate));
    if (settings.num_executors > 0)
      conf.set("spark.executor.instances", String.format("%d", settings.num_executors));
    if (settings.num_executor_cores > 0)
      conf.set("spark.executor.cores", String.format("%d", settings.num_executor_cores));
    if (!settings.executor_memory.isEmpty())
      conf.set("spark.executor.memory", settings.executor_memory);
    conf.set("spark.locality.wait", settings.locality_wait);
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    conf.set("spark.sql.warehouse.dir", warehouseLocation);
    // conf.set("spark.sql.shuffle.partitions", Integer.toString(settings.num_executors));
    // conf.set("spark.default.parallelism", Integer.toString(settings.num_executors));
    conf.set("spark.shuffle.reduceLocality.enabled", "false");
    conf.set("spark.sql.streaming.schemaInference", "true");
    conf.set("spark.locality.wait", "30s"); // FIX: Allow time to load local DBs
    // -> process, node, rack, any
    conf.set("spark.scheduler.mode", "FAIR"); // FIX

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

    ArrayList<Row> data = new ArrayList<Row>(settings.num_db_partitions);

    for (int i = 0; i < settings.num_db_partitions; i++) {
      Row r = RowFactory.create("nt", Integer.valueOf(i));
      data.add(r);
    }

    Dataset<Row> parts = sparksession.createDataFrame(data, parts_schema);
    Dataset<Row> blast_partitions =
        parts
            // .repartition(settings.num_executors, parts.col("partition_num"))
            .repartition(settings.num_db_partitions, parts.col("partition_num"))
            .persist(StorageLevel.MEMORY_AND_DISK());

    blast_partitions.show();
    blast_partitions.createOrReplaceTempView("blast_partitions");

    return true;
  }

  private static DataStreamWriter<Row> make_prelim_stream() {
    System.out.println("making prelim_stream");

    DataStreamReader query_stream = sparksession.readStream();
    query_stream.format("json");
    query_stream.option("maxFilesPerTrigger", 1); // FIX: Configureable
    query_stream.option("multiLine", true);
    query_stream.option("includeTimestamp", true);

    Dataset<Row> queries = query_stream.json(settings.hdfs_source_dir);
    queries.printSchema();
    queries.createOrReplaceTempView("queries");

    Dataset<Row> joined =
        sparksession.sql(
            "select RID, queries.db, partition_num, "
                + "query_seq, timestamp_hdfs "
                + "from queries, blast_partitions "
                + "where queries.db=blast_partitions.db "
                + "distribute by partition_num");
    //           .repartition(settings.num_executors);
    //    joined.createOrReplaceTempView("joined");
    joined.printSchema();

    Integer top_n = settings.top_n;
    String jni_log_level = settings.jni_log_level;
    String hsp_result_dir = settings.hdfs_result_dir + "/hsps";
    String db_location = settings.db_location;

    BLAST_SETTINGS settings_closure = settings;

    Dataset<Row> prelim_search_results =
        joined
            .flatMap( // FIX: Make a functor
                (FlatMapFunction<Row, String>)
                    inrow -> {
                      Logger logger = LogManager.getLogger(BLAST_DRIVER.class);

                      String rid = inrow.getString(inrow.fieldIndex("RID"));
                      logger.log(Level.INFO, "Flatmapped RID " + rid);
                      String db = inrow.getString(inrow.fieldIndex("db"));
                      int partition_num = inrow.getInt(inrow.fieldIndex("partition_num"));
                      String query_seq = inrow.getString(inrow.fieldIndex("query_seq"));
                      logger.log(
                          Level.INFO,
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
                      logger.log(Level.INFO, "<row> is :" + inrow.mkString(":"));

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
    //        prelim_search_results.createOrReplaceTempView("prelim_search_results");
    Dataset<Row> prelim_search_1 = prelim_search_results.repartition(1);

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
                    score_map = new HashMap<>(); // String, TreeMap<Integer, ArrayList<String>>>();
                    this.partitionId = partitionId;
                    logger = LogManager.getLogger(BLAST_DRIVER.class);
                    try {
                      Configuration conf = new Configuration();
                      fs = FileSystem.get(conf);
                    } catch (IOException e) {
                      System.err.println(e);
                      return false;
                    }

                    logger.log(Level.DEBUG, String.format("open %d %d", partitionId, version));
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
                    logger.log(Level.DEBUG, "  line is " + line.substring(0, 50));

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
                      StringBuilder output = new StringBuilder(tm.size() * 1000);
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
                      write_to_hdfs(rid, output.toString());
                    }

                    logger.log(Level.DEBUG, String.format("close %d", partitionId));
                    try {
                      fs.close();
                    } catch (IOException ioe) {
                      logger.log(Level.DEBUG, "Couldn't close HDFS filesystem");
                    }

                    return;
                  } // close

                  private void write_to_hdfs(String rid, String output) {
                    try {
                      /*
                      SecureRandom random = new SecureRandom();
                      byte[] bytes = new byte[10];
                      random.nextBytes(bytes);
                      byte[] encoded = Base64.getEncoder().encode(bytes);
                      */
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
                          Level.DEBUG,
                          String.format("Wrote %d bytes to HDFS %s", output.length(), outfile));

                    } catch (IOException ioe) {
                      logger.log(Level.DEBUG, "Couldn't write to HDFS");
                      logger.log(Level.DEBUG, ioe.toString());
                    }
                  } // write_to_hdfs
                } // ForeachWriter
                ) // foreach
            .outputMode("Append");

    System.out.println("made  prelim_stream");
    return topn_dsw;
  }

  private static boolean run_streams(DataStreamWriter<Row> prelim_dsw) {
    System.out.println("starting streams...");
    //  StreamingQuery prelim_results = prelim_dsw.outputMode("append").format("console").start();
    try {
      for (int i = 0; i < 10; ++i) {
        StreamingQuery results = prelim_dsw.start();
        System.out.println("stream running...");
        Thread.sleep(30000);
        System.out.println(results.lastProgress());
        System.out.println(results.status());
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
      ;
    }

    if (!make_partitions()) {
      shutdown();
      System.exit(2);
    }

    DataStreamWriter<Row> prelim_stream = make_prelim_stream();
    result = run_streams(prelim_stream);
    if (!result) {
      shutdown();
      System.exit(3);
    }
  }
}
