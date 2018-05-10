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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.TreeMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataStreamWriter;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

class BLAST_DRIVER extends Thread {
  private final BLAST_SETTINGS settings;
  private final BLAST_YARN_NODES nodes;
  private final SparkSession ss;
  private final SparkContext sc;
  private final JavaSparkContext jsc;

  public BLAST_DRIVER(final BLAST_SETTINGS settings, final List<String> files_to_transfer) {
    this.settings = settings;
    this.nodes = new BLAST_YARN_NODES(); // discovers yarn-nodes

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
    for (String a_file : files_to_transfer) sc.addFile(a_file);

    // create a streaming-context from SparkContext given
    //        jssc = new JavaStreamingContext(sc, Durations.seconds(settings.batch_duration));
  }

  public void stop_blast() {
    System.out.println("stop_blast()");
    try {
      if (jsc != null) jsc.stop();
    } catch (Exception e) {
      System.out.println("JavaStreamingContext.stop() : " + e);
    }
  }

  private Integer node_count() {
    Integer res = nodes.count();
    if (res == 0) res = settings.num_executors;
    return res;
  }

  @Override
  public void run() {
    System.out.println("in run()");

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
    query_stream.option("maxFilesPerTrigger", 10);
    query_stream.option("multiLine", true);
    query_stream.option("includeTimestamp", true);

    Dataset<Row> queries = query_stream.json("/user/vartanianmh/requests");
    queries.printSchema();
    queries.createOrReplaceTempView("queries");

    Dataset<Row> joined =
        ss.sql(
                "select RID, queries.db, query_seq, num from queries, parts2 where queries.db=parts2.db distribute by num")
            .repartition(1000);
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

    Dataset<Row> out3 = ss.sql("select * from out2");
    //        Dataset<String> out3=out2.toDF();

    DataStreamWriter<String> dsw = out3.writeStream();
    DataStreamWriter<String> dsw2 = dsw.foreach();
    DataStreamWriter<String> dsw3 = dsw2.outputMode("Append");
    DataStreamWriter<String> dsw4 = dsw3.start();

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

    //        StreamingQuery results =
    // out2.writeStream().outputMode("append").format("console").start();

    System.out.println("driver starting...");
    try {
      results.awaitTermination();
      System.out.println("driver started...");
    } catch (Exception e) {
      System.out.println("Spark exception: " + e);
    }
  }
}
