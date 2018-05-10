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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.TreeMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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

public final class BLAST_DRIVER {
  public static void main(String[] args) throws Exception {
    BLAST_SETTINGS settings;
    SparkSession ss;
    SparkContext sc;
    JavaSparkContext jsc;

    if (args.length != 1) {
      System.out.println("settings json-file missing");
      return;
    }
    String ini_path = args[0];

    String appName = "experiment_mike";
    settings = BLAST_SETTINGS_READER.read_from_json(ini_path, appName);
    System.out.println(String.format("settings read from '%s'", ini_path));
    if (!settings.valid()) {
      System.out.println(settings.missing());
      return;
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
    // conf.set("spark.sql.shuffle.partitions", "900");
    // conf.set("spark.default.parallelism", "900");
    conf.set("spark.shuffle.reduceLocality.enabled", "false");
    conf.set("spark.sql.streaming.schemaInference", "true");

    builder.config(conf);
    System.out.println(conf.toDebugString());
    System.out.println();
    builder.enableHiveSupport();

    ss = builder.getOrCreate();
    sc = ss.sparkContext();
    jsc = new JavaSparkContext(sc);

    sc.setLogLevel(settings.spark_log_level);

    // send the given files to all nodes
    List<String> files_to_transfer = new ArrayList<>();
    files_to_transfer.add("libblastjni.so");
    for (String a_file : files_to_transfer) sc.addFile(a_file);

    System.out.println("starting");

    //        StructType query_schema = StructType.fromDDL("RID string, db string, query_seq
    // string");

    StructType parts_schema = StructType.fromDDL("db string, num int");

    ArrayList<Row> data = new ArrayList<Row>();

    for (int i = 0; i < settings.num_db_partitions; i++) {
      Row r = RowFactory.create("nt", new Integer(i));
      data.add(r);
    }
    Dataset<Row> parts2 = ss.createDataFrame(data, parts_schema);
    parts2.createOrReplaceTempView("parts2");
    Dataset<Row> parts3 = parts2.repartition(886, parts2.col("num")).cache();
    //       return sc.parallelizePairs(db_list, node_count())

    // Dataset<Row> parts=ss.read().json("/user/vartanianmh/parts.json");
    parts3.show();
    // Dataset<Row> parts2=ss.sql("select * from parts distribute by num");
    //    Dataset<Row> parts2=parts.repartition(886,parts.col("num"));
    //    parts2.cache();
    parts3.createOrReplaceTempView("parts3");
    //    dsquery.show();

    DataStreamReader query_stream = ss.readStream();
    query_stream.format("json");
    //        query_stream.schema(query_schema);
    query_stream.option("maxFilesPerTrigger", 5);
    query_stream.option("multiLine", true);
    query_stream.option("includeTimestamp", true);

    Dataset<Row> queries = query_stream.json("/user/vartanianmh/requests");
    queries.printSchema();
    queries.createOrReplaceTempView("queries");

    Dataset<Row> joined =
        ss.sql(
                "select RID, queries.db, query_seq, num from queries, parts2 where queries.db=parts2.db distribute by num")
            .repartition(200);
    joined.createOrReplaceTempView("joined");
    joined.printSchema();

    //        StructType out_schema=StructType.fromDDL("foo string, foo2 string");
    //        ExpressionEncoder<Row> encoder = RowEncoder.apply(out_schema);

    Dataset<Row> out2 =
        joined
            .flatMap(
                (FlatMapFunction<Row, String>)
                    inrow -> {
                      long starttime = System.currentTimeMillis();
                      BLAST_LIB blaster = new BLAST_LIB();
                      blaster.log("INFO", "SPARK:" + inrow.mkString(":"));

                      String rid = inrow.getString(0);
                      String db = inrow.getString(1);
                      String query_seq = inrow.getString(2);
                      int num = -999;
                      if (!inrow.isNullAt(3)) num = inrow.getInt(3);

                      BLAST_REQUEST requestobj = new BLAST_REQUEST();
                      requestobj.id = "test ";
                      requestobj.query_seq = query_seq;
                      requestobj.query_url = "";
                      requestobj.params = "nt:" + inrow.mkString();
                      requestobj.db = "nt";
                      requestobj.program = "blastn";
                      requestobj.top_n = 100;
                      BLAST_PARTITION partitionobj =
                          new BLAST_PARTITION("/tmp/blast/db/prefetched", "nt_50M", num, false);

                      List<String> result = new ArrayList<>();
                      if (blaster != null) {
                        BLAST_HSP_LIST[] search_res =
                            blaster.jni_prelim_search(partitionobj, requestobj, "INFO");
                        // result.add(String.format("Got %d", search_res.length));

                        for (BLAST_HSP_LIST S : search_res) {
                          // String rec=String.format("%d, %d = %d", S.oid, S.max_score,
                          // S.hsp_blob.length);
                          byte[] encoded = Base64.getEncoder().encode(S.hsp_blob);
                          String b64blob = new String(encoded);
                          String rec = String.format("%d,%s", S.max_score, b64blob);
                          result.add(rec);
                        }
                        long finishtime = System.currentTimeMillis();
                        // String bench=String.format("Took %d", finishtime-starttime);
                        // result.add(bench);

                      } else result.add("null blaster ");
                      return result.iterator();
                    },
                Encoders.STRING())
            .toDF("fromflatmap");
    out2.createOrReplaceTempView("out2");
    /*
    Dataset<Row> scores = ss.sql("select '2018-01-01 01:00:00.0' as timestamp2, 'fakerid' as rid, fromflatmap as score from out2");
    scores.createOrReplaceTempView("scores");

    Dataset<Row> windows=scores.groupBy(
    functions.window(
    scores.col("timestamp2"), "1 seconds"),
    scores.col("rid")).count().orderBy("window");
    windows.createOrReplaceTempView("windows");

    Dataset<Row> topn = ss.sql("select rid, dense_rank() over (partition by rid order by rid desc) as rk from windows");
    */
    /*
    Dataset<Row> qp = ss.sql("select concat(num,' ',query_seq) as combo from joined").repartition(886);
    Dataset<String> qs = qp.as(Encoders.STRING());
    Dataset<Row> out =
    qs.flatMap(
    (FlatMapFunction<String, String>)
    t -> {
    String[] ts = t.split(" ");

    BLAST_REQUEST requestobj = new BLAST_REQUEST();
    requestobj.id = "test ";
    requestobj.query_seq = ts[0];
    requestobj.query_seq = t;
    requestobj.query_url = "";
    requestobj.params = "nt:" + t + t.length() + " " + ts.length;
    requestobj.db = "nt";
    requestobj.program = "blastn";
    requestobj.top_n = 100;
    BLAST_PARTITION partitionobj =
    new BLAST_PARTITION(
    "/tmp/blast/db/prefetched", "nt_50M", Integer.parseInt(ts[0]), false);

    BLAST_LIB blaster =
    new BLAST_LIB(); // BLAST_LIB_SINGLETON.get_lib(part, bls);
    blaster.log("INFO", "hi there");

    List<String> result = new ArrayList<>();
    if (blaster != null) {
    BLAST_HSP_LIST[] search_res =
    blaster.jni_prelim_search(partitionobj, requestobj, "INFO");

    for (BLAST_HSP_LIST S : search_res) result.add(S.toString());
    } else result.add("null blaster " + t);
    return result.iterator();
    },
    Encoders.STRING())
    .toDF("fromflatmap");
    */

    //    Dataset<Row> out3 = ss.sql("select * from out2 distribute by 1");
    Dataset<Row> out3 = out2.repartition(1);
    //        Dataset<String> out3=out2.toDF();

    DataStreamWriter<Row> dsw = out3.writeStream();
    DataStreamWriter<Row> dsw2 =
        dsw.foreach(
            new ForeachWriter<Row>() {
              private TreeMap<Integer, String> tmap;
              private long starttime;
              private long finishtime;
              private PrintWriter out;
              private long partitionId;
              private int recordcount = 0;

              @Override
              public boolean open(long partitionId, long version) {
                starttime = System.currentTimeMillis();
                tmap = new TreeMap<Integer, String>();

                this.partitionId = partitionId;

                try {
                  out = new PrintWriter(new FileWriter("/tmp/mike2.log", true), true);
                  out.println(String.format("open %d %d", partitionId, version));
                  if (partitionId != 0)
                    out.println(String.format(" *** not partition 0 %d ??? ", partitionId));
                } catch (IOException e) {
                  System.err.println(e);
                }
                return true;
              }

              @Override
              public void process(Row value) {
                ++recordcount;
                out.println(String.format(" in process %d", partitionId));
                out.println("  " + value.mkString(":"));
                String line = value.getString(0);
                out.println("  line is " + line);
                String[] csv = line.split(",");
                out.println("  has " + csv.length);
                Integer score = Integer.parseInt(csv[0]);
                out.println(String.format("score is %d", score));
                tmap.put(score, csv[1]);
                // write string to connection
              }

              @Override
              public void close(Throwable errorOrNull) {
                finishtime = System.currentTimeMillis();
                out.println(String.format(" foreach finished at %d", finishtime));
                out.println(String.format(" foreach loop took %d ms", finishtime - starttime));
                out.println(String.format(" saw %d records", recordcount));
                out.println(String.format(" treemap had %d", tmap.size()));
                out.println(String.format("close %d", partitionId));
                out.flush();
                // close the connection
              }
            });
    DataStreamWriter<Row> dsw3 = dsw2.outputMode("Append");
    /*
       StreamingQuery results =
       out3.writeStream()
       .foreach(
       new ForeachWriter<Row>() {
       private TreeMap<Integer, String> tmap;

       @Override
       public boolean open(long partitionId, long version) {
       BLAST_LIB blaster = new BLAST_LIB();
       blaster.log("INFO", String.format("in open %d %d", partitionId, version));
       return true;
       }

       @Override
       public void process(Row value) {
    // write string to connection
       }

       @Override
       public void close(Throwable errorOrNull) {
    // close the connection
       }
       })
       .start();
       */
    StreamingQuery results2 = out3.writeStream().outputMode("append").format("console").start();

    System.out.println("starting stream...");
    try {
      for (int i = 0; i < 9; ++i) {
        StreamingQuery results = dsw3.start();
        System.out.println("stream running...");
        Thread.sleep(10000);
        System.out.println(results.lastProgress());
        System.out.println(results.status());
      }
    } catch (Exception e) {
      System.out.println("Spark exception: " + e);
    }
    System.out.println("That is enough for now");
  } // run

  /*

         try {
         wait_for_console("exit", 500);
         driver.stop_blast();
         driver.join();
         } catch (InterruptedException e1) {
         System.out.println(String.format("driver interrupted: %s", e1));
         } catch (Exception e2) {
         System.out.println(String.format("stopping driver: %s", e2));
         }
  }
  }

  */

}
