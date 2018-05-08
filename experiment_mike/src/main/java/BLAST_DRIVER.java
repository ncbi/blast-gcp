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

import static java.lang.Math.min;

import com.google.cloud.spark.pubsub.PubsubUtils;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.ByteBuffer;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStream$;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.StreamingQuery;
import java.util.Arrays;
import java.util.Iterator;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;


class BLAST_DRIVER extends Thread {
    private final BLAST_SETTINGS settings;
    private final BLAST_YARN_NODES nodes;
    private final SparkSession ss;
    private final SparkContext sc;
    private final JavaSparkContext jsc;
    private final JavaStreamingContext jssc;

    public BLAST_DRIVER(final BLAST_SETTINGS settings, final List<String> files_to_transfer) {
        this.settings = settings;
        this.nodes = new BLAST_YARN_NODES(); // discovers yarn-nodes

        SparkSession.Builder builder=new SparkSession.Builder();
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
        conf.set("spark.sql.shuffle.partitions", "300");
        conf.set("spark.default.parallelism", "300");
        conf.set("spark.shuffle.reduceLocality.enabled", "false");

        builder.config(conf);
        System.out.println(conf.toDebugString());
        builder.enableHiveSupport();

        ss=builder.getOrCreate();
        sc=ss.sparkContext();
        jsc=new JavaSparkContext(sc);

        sc.setLogLevel(settings.spark_log_level);

        // send the given files to all nodes
        for (String a_file : files_to_transfer) sc.addFile(a_file);

        // create a streaming-context from SparkContext given
        //        jssc = new JavaStreamingContext(sc, Durations.seconds(settings.batch_duration));
        jssc=null;
    }

    public void stop_blast() {
        System.out.println("stop_blast()");
        try {
            if (jssc != null) jssc.stop(true, true);
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

    /*
       private JavaRDD<BLAST_PARTITION> make_db_partitions_1(
       Broadcast<BLAST_SETTINGS> SETTINGS, Broadcast<BLAST_PARTITIONER0> PARTITIONER0) {
       final List<Tuple2<Integer, BLAST_PARTITION>> db_list = new ArrayList<>();
       for (int i = 0; i < settings.num_db_partitions; i++) {
       BLAST_PARTITION part =
       new BLAST_PARTITION(
       settings.db_location, settings.db_pattern, i, settings.flat_db_layout);
       db_list.add(new Tuple2<>(i, part));
       }

       return sc.parallelizePairs(db_list, node_count())
       .partitionBy(PARTITIONER0.getValue())
       .map(
       item -> {
       BLAST_PARTITION part = item._2();

       BLAST_SETTINGS bls = SETTINGS.getValue();
       if (bls.log_part_prep) BLAST_SEND.send(bls, String.format("preparing %s", part));

       return BLAST_LIB_SINGLETON.prepare(part, bls);
       })
       .cache();
       }

       private JavaRDD<BLAST_PARTITION> make_db_partitions_2(Broadcast<BLAST_SETTINGS> SETTINGS) {
       final List<Tuple2<BLAST_PARTITION, Seq<String>>> part_list = new ArrayList<>();
       for (int i = 0; i < settings.num_db_partitions; i++) {
       BLAST_PARTITION part =
       new BLAST_PARTITION(
       settings.db_location, settings.db_pattern, i, settings.flat_db_layout);

       String host = nodes.getHost(part.getPartition(node_count()));

       if (settings.log_pref_loc)
       BLAST_SEND.send(settings, String.format("adding %s for %s", host, part));

       List<String> prefered_loc = new ArrayList<>();
       prefered_loc.add(host);

       Seq<String> prefered_loc_seq = JavaConversions.asScalaBuffer(prefered_loc).toSeq();

       part_list.add(new Tuple2<>(part, prefered_loc_seq));
       }

// we transform thes list into a Seq
final Seq<Tuple2<BLAST_PARTITION, Seq<String>>> temp1 =
JavaConversions.asScalaBuffer(part_list).toSeq();

// we need the class-tag for twice for conversions
ClassTag<BLAST_PARTITION> tag = scala.reflect.ClassTag$.MODULE$.apply(BLAST_PARTITION.class);

// this will distribute the partitions to different worker-nodes
final RDD<BLAST_PARTITION> temp2 = sc.toSparkContext(sc).makeRDD(temp1, tag);

// now we transform it into a JavaRDD and perform a mapping-op to eventuall load the
// database onto the worker-node ( if it is not already there )
return JavaRDD.fromRDD(temp2, tag)
.map(
item -> {
BLAST_SETTINGS bls = SETTINGS.getValue();

if (bls.log_part_prep) BLAST_SEND.send(bls, String.format("preparing %s", item));

return BLAST_LIB_SINGLETON.prepare(item, bls);
})
.cache();
       }

       private JavaDStream<String> create_socket_stream() {
       return jssc.socketTextStream(settings.trigger_host, settings.trigger_port).cache();
       }

private JavaDStream<String> create_pubsub_stream() {
    final DStream<PubsubMessage> pub1 =
        PubsubUtils.createStream(jssc.ssc(), settings.project_id, settings.subscript_id);
    final JavaDStream<PubsubMessage> pub2 =
        JavaDStream$.MODULE$.fromDStream(
                pub1, scala.reflect.ClassTag$.MODULE$.apply(PubsubMessage.class));

    return pub2.map(item -> item.getData().toStringUtf8()).cache();
}

private JavaDStream<String> create_file_stream() {
    return jssc.textFileStream(settings.hdfs_source_dir).cache();
}

private JavaDStream<BLAST_REQUEST> create_source(Broadcast<BLAST_SETTINGS> SETTINGS) {
    JavaDStream<String> tmp = null;

    JavaDStream<String> S3 = create_file_stream();
}

private JavaPairDStream<String, BLAST_HSP_LIST> perform_prelim_search(
        final JavaPairDStream<BLAST_PARTITION, BLAST_REQUEST> SRC,
        Broadcast<BLAST_SETTINGS> SETTINGS) {
    return SRC.flatMapToPair(
            item -> {
                BLAST_SETTINGS bls = SETTINGS.getValue();

                BLAST_PARTITION part = item._1();
                BLAST_REQUEST req = item._2();

                // see if we are at a different worker-id now
                if (bls.log_worker_shift) {
                    String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                    if (!curr_worker_name.equals(part.worker_name)) {
                        BLAST_SEND.send(
                                bls,
                                String.format(
                                    "pre worker-shift for %d: %s -> %s",
                                    part.nr, part.worker_name, curr_worker_name));
                    }
                }

                // ++++++ this is the where the work happens on the worker-nodes ++++++
                if (bls.log_job_start)
                    BLAST_SEND.send(
                            bls, String.format("starting request: '%s' at '%s' ", req.id, part.db_spec));

                ArrayList<Tuple2<String, BLAST_HSP_LIST>> ret = new ArrayList<>();
                try {
                    BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib(part, bls);

                    if (blaster != null) {
                        long startTime = System.currentTimeMillis();
                        BLAST_HSP_LIST[] search_res =
                            blaster.jni_prelim_search(part, req, bls.jni_log_level);
                        long elapsed = System.currentTimeMillis() - startTime;

                        for (BLAST_HSP_LIST S : search_res)
                            ret.add(new Tuple2<>(String.format("%d %s", part.nr, req.id), S));

                        if (bls.log_job_done)
                            BLAST_SEND.send(
                                    bls,
                                    String.format(
                                        "request '%s'.'%s' done -> count = %d",
                                        req.id, part.db_spec, search_res.length));
                    }
                } catch (Exception e) {
                    BLAST_SEND.send(
                            bls,
                            String.format(
                                "request exeption: '%s on %s' for '%s'",
                                e, req.toString(), part.toString()));
                }
                return ret.iterator();
            })
    .cache();
        }


private JavaPairDStream<String, Integer> calculate_cutoff(
        final JavaPairDStream<String, BLAST_HSP_LIST> HSPS, Broadcast<BLAST_SETTINGS> SETTINGS) {
    // key   : req_id
    // value : scores, each having a copy of N for picking the cutoff value
    final JavaPairDStream<String, BLAST_RID_SCORE> TEMP1 =
        HSPS.mapToPair(
                item -> {
                    String key = item._1(); 
                    BLAST_HSP_LIST hsp_list = item._2();

                    String req_id = key.substring(key.indexOf(' ') + 1, key.length());

                    BLAST_RID_SCORE ris = new BLAST_RID_SCORE(hsp_list.max_score, hsp_list.req.top_n);
                    return new Tuple2<String, BLAST_RID_SCORE>(req_id, ris);
                })
    .cache();

    // key   : req_id
    // value : list of scores, each having a copy of N for picking the cutoff value
    // this will conentrate all BLAST_RID_SCORE instances for a given req_id on one worker node
    final JavaPairDStream<String, Iterable<BLAST_RID_SCORE>> TEMP2 = TEMP1.groupByKey().cache();

    // key   : req_id
    // value : the cutoff value
    // this will run once for a given req_id on one worker node
    return TEMP2
        .mapToPair(
                item -> {
                    Integer cutoff = 0;
                    Integer top_n = 0;
                    ArrayList<Integer> lst = new ArrayList<>();
                    for (BLAST_RID_SCORE s : item._2()) {
                        if (top_n == 0) top_n = s.top_n;
                        lst.add(s.score);
                    }
                    Collections.sort(lst, Collections.reverseOrder());

                    Integer nr = (lst.size() > top_n) ? top_n - 1 : lst.size() - 1;
                    cutoff = lst.get(nr);

                    BLAST_SETTINGS bls = SETTINGS.getValue();
                    if (bls.log_cutoff)
                        BLAST_SEND.send(bls, String.format("CUTOFF[ %s ] : %d", item._1(), cutoff));
                    return new Tuple2<>(item._1(), cutoff);
                })
    .cache();
        }

private JavaPairDStream<String, Tuple2<BLAST_HSP_LIST, Integer>> filter_by_cutoff(
        JavaPairDStream<String, BLAST_HSP_LIST> HSPS,
        JavaPairDStream<String, Integer> CUTOFF,
        Broadcast<BLAST_PARTITIONER2> PARTITIONER2) {
    JavaPairDStream<String, String> ACTIVE_PARTITIONS =
        HSPS.transform(rdd -> rdd.keys().distinct())
        .mapToPair(
                item -> new Tuple2<>(item.substring(item.indexOf(' ') + 1, item.length()), item));

    JavaPairDStream<String, Integer> CUTOFF2 =
        ACTIVE_PARTITIONS
        .join(CUTOFF)
        .mapToPair(item -> item._2())
        .transformToPair(rdd -> rdd.partitionBy(PARTITIONER2.getValue()));

    return HSPS.join(CUTOFF2)
        .filter(
                item -> {
                    BLAST_HSP_LIST hsps = item._2()._1();
                    Integer cutoff = item._2()._2();
                    return (cutoff == 0 || hsps.max_score >= cutoff);
                })
    .cache();

    /*
       return HSPS.join( CUTOFF2, PARTITIONER2.getValue() ) . filter( item ->
       {
       BLAST_HSP_LIST hsps = item._2()._1();
       Integer cutoff = item._2()._2();
       return ( cutoff == 0 || hsps.max_score >= cutoff );
       }).cache();
        }

        private JavaPairDStream<String, BLAST_TB_LIST> perform_traceback(
        JavaPairDStream<String, Tuple2<BLAST_HSP_LIST, Integer>> HSPS,
        Broadcast<BLAST_PARTITIONER2> PARTITIONER2,
        Broadcast<BLAST_SETTINGS> SETTINGS) {
        JavaPairDStream<String, BLAST_HSP_LIST> temp1 = HSPS.mapValues(item -> item._1());

        JavaPairDStream<String, Iterable<BLAST_HSP_LIST>> temp2 =
        temp1.groupByKey(PARTITIONER2.getValue());

        return temp2
        .flatMapToPair(
        item -> {
        String key = item._1();

        BLAST_SETTINGS bls = SETTINGS.getValue();

        ArrayList<BLAST_HSP_LIST> all_gcps = new ArrayList<>();
        for (BLAST_HSP_LIST e : item._2()) all_gcps.add(e);

        BLAST_HSP_LIST[] a = new BLAST_HSP_LIST[all_gcps.size()];
        int i = 0;
        for (BLAST_HSP_LIST e : all_gcps) a[i++] = e;

        BLAST_PARTITION part = a[0].part;

        BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib(part, bls);

        if (bls.log_worker_shift) {
        String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
        if (!curr_worker_name.equals(part.worker_name)) {
        BLAST_SEND.send(
        bls,
        String.format(
        "tb worker-shift for %d: %s -> %s",
        part.nr, part.worker_name, curr_worker_name));
        }
        }

        BLAST_TB_LIST[] results = blaster.jni_traceback(a, part, a[0].req, bls.jni_log_level);

        ArrayList<Tuple2<String, BLAST_TB_LIST>> ret = new ArrayList<>();
        for (BLAST_TB_LIST L : results) ret.add(new Tuple2<>(key, L));

        return ret.iterator();
        })
        .cache();
        };

        private void write_traceback(
        JavaPairDStream<String, BLAST_TB_LIST> SEQANNOTS, Broadcast<BLAST_SETTINGS> SETTINGS) {
    // key is now req-id
    final JavaPairDStream<String, BLAST_TB_LIST> SEQANNOTS1 =
    SEQANNOTS.mapToPair(
    item -> {
    String key = item._1();
    return new Tuple2<>(key.substring(key.indexOf(' ') + 1, key.length()), item._2());
    });

    final JavaPairDStream<String, Iterable<BLAST_TB_LIST>> SEQANNOTS2 = SEQANNOTS1.groupByKey();

    final JavaPairDStream<String, ByteBuffer> SEQANNOTS3 =
    SEQANNOTS2.mapValues(
    item -> {
        List<BLAST_TB_LIST> all_seq_annots = new ArrayList<>();
        for (BLAST_TB_LIST e : item) all_seq_annots.add(e);

        Collections.sort(all_seq_annots);
        Integer top_n = all_seq_annots.get(0).req.top_n;

        List<BLAST_TB_LIST> top_n_seq_annots =
            all_seq_annots.subList(0, min(all_seq_annots.size(), top_n));
        int sum = 0;
        for (BLAST_TB_LIST e : top_n_seq_annots) sum += e.asn1_blob.length;

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
        ByteBuffer ret =
            ByteBuffer.allocate(sum + seq_annot_prefix.length + seq_annot_suffix.length);

        ret.put(seq_annot_prefix);
        for (BLAST_TB_LIST e : top_n_seq_annots) ret.put(e.asn1_blob);
        ret.put(seq_annot_suffix);

        return ret;
    });

SEQANNOTS3.foreachRDD(
        rdd -> {
            long count = rdd.count();
            if (count > 0) {
                rdd.foreachPartition(
                        iter -> {
                            while (iter.hasNext()) {
                                Tuple2<String, ByteBuffer> item = iter.next();
                                String req_id = item._1();
                                ByteBuffer value = item._2();

                                BLAST_SETTINGS bls = SETTINGS.getValue();

                                if (bls.gs_result_bucket.isEmpty()) {
                                    String fn = String.format(bls.hdfs_result_file, req_id);
                                    String path = String.format("%s/%s", bls.hdfs_result_dir, fn);
                                    Integer uploaded = BLAST_HADOOP_UPLOADER.upload(path, value);

                                    if (bls.log_final)
                                        BLAST_SEND.send(
                                                bls, String.format("%d bytes written to hadoop '%s'", uploaded, path));
                                } else {
                                    String gs_result_key = String.format(bls.gs_result_file, req_id);
                                    Integer uploaded =
                                        BLAST_GS_UPLOADER.upload(bls.gs_result_bucket, gs_result_key, value);

                                    String gs_status_key = String.format(bls.gs_status_file, req_id);
                                    BLAST_GS_UPLOADER.upload(
                                            bls.gs_status_bucket, gs_status_key, bls.gs_status_done);

                                    if (bls.log_final)
                                        BLAST_SEND.send(
                                                bls,
                                                String.format(
                                                    "%d bytes written to gs '%s':'%s'",
                                                    uploaded, bls.gs_result_bucket, gs_result_key));
                                }
                            }
                        });
            }
        });
        }
*/
@Override
public void run()  {
    System.out.println("in run()");

    Dataset<Row> dsquery=ss.read().format("json").load("/user/vartanianmh/sample.json");
    StructType schema=dsquery.schema();
    dsquery.printSchema();

    Dataset<Row> parts=ss.read().json("/user/vartanianmh/parts.json");
    parts.show();
    parts.createOrReplaceTempView("parts");
    Dataset<Row> parts2=ss.sql("select * from parts distribute by num");
//    Dataset<Row> parts2=parts.repartition(886,parts.col("num"));
//    parts2.cache();
    parts2.createOrReplaceTempView("parts2");
    //    dsquery.show();

    /*
       Dataset<Row> queries=ss.readStream()
       .schema(schema).json("/user/vartanianmh/requests");
       */
    /*
       .format("json")
       .load("/user/vartanianmh/requests/");
       */
    DataStreamReader dsr=ss.readStream();
    dsr.format("json");
    dsr.schema(schema);
    dsr.option("maxFilesPerTrigger",1);
    //    dsr.option("includeTimetamp",true);
    dsr.option("multiLine",false);

    Dataset<Row> queries=dsr.json("/user/vartanianmh/requests");
    queries.createOrReplaceTempView("queries");

    Dataset<Row> joined=ss.sql("select * from queries, parts2 where queries.db=parts2.db distribute by num");
    joined.createOrReplaceTempView("joined");
    //    Dataset<Row> joined=queries.join(parts2,"db");

    Dataset<Row> qp=ss.sql("select concat(num,query) as combo from joined").repartition(886);
    Dataset<String> qs=qp.as(Encoders.STRING());

    Dataset<Row> out=qs.flatMap((FlatMapFunction<String, String>) t-> {
        String[]ts=t.split(" ");

        BLAST_REQUEST requestobj = new BLAST_REQUEST();
        requestobj.id = "test ";
        requestobj.query_seq = ts[2];
        requestobj.query_url = "";
        requestobj.params = "nt:" + t;
        requestobj.db = "nt";
        requestobj.program = "blastn";
        requestobj.top_n = 100;
        BLAST_PARTITION partitionobj = new BLAST_PARTITION("/tmp/blast/db", "nt_50M", Integer.parseInt(ts[1]), false);

        BLAST_LIB blaster = new BLAST_LIB(); //BLAST_LIB_SINGLETON.get_lib(part, bls);

        List<String> result=new ArrayList<>();
        if (blaster != null) {
            BLAST_HSP_LIST[] search_res =
                blaster.jni_prelim_search(partitionobj, requestobj, "DEBUG");

            for (BLAST_HSP_LIST S : search_res)
                result.add(S.toString());
        } else result.add("null blaster " + t);
        return result.iterator();
    }, Encoders.STRING() ).toDF("fromflatmap");

    /*
     *

     *
    // Split the lines into words, retaining timestamps
    Dataset<Row> words = lines
    .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
    .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timest^amp>>) t -> {
    List<Tuple2<String, Timestamp>> result = new ArrayList<>();
    for (String word : t._1.split(" ")) {
    result.add(new Tuple2<>(word, t._2));
    }
    return result.iterator();
    },
    Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
    ).toDF("word", "timestamp");


    Dataset<Row> pm=qp
    FlatMapFunction<String, String> mymap=
    new FlatMapFunction<String, String>() {
    @Override
    public Iterator<String> call (String str) throws
    */
    /*
       FlatMapFunction<Row, Row> mymap=
       new FlatMapFunction<Row, Row>() {
       @Override
       public Iterator<Row> call(Row row) throws Exception {
       ArrayList<row> rowList = newArrayList<Row>();
       rowList.add(row);
       return rowList.iterator();
       };
       */
    //    Dataset<Row> maps=joined.flatMap(mymap, Encoders.kryo(Row.class));
    //Dataset<String> q=joined.as(Encoders.STRING());

    //    Dataset<String> f=joined.selectExpr("CAST(query) as STRING").as(Encoders.STRING());

    StreamingQuery results = out.writeStream()
        .outputMode("append")
        .format("console")
        .start();

    System.out.println("driver starting...");
    try {
        results.awaitTermination();
        System.out.println("driver started...");
    }
    catch (Exception e) {
        System.out.println("Spark exception: " + e);
    }

       }

/*
   Broadcast<BLAST_SETTINGS> SETTINGS = sc.broadcast(settings);
   Broadcast<BLAST_PARTITIONER0> PARTITIONER0 =
   sc.broadcast(new BLAST_PARTITIONER0(node_count()));
   Broadcast<BLAST_PARTITIONER1> PARTITIONER1 =
   sc.broadcast(new BLAST_PARTITIONER1(node_count()));
   Broadcast<BLAST_PARTITIONER2> PARTITIONER2 =
   sc.broadcast(new BLAST_PARTITIONER2(node_count()));

   JavaRDD<BLAST_PARTITION> DB_SECS;
   if (settings.with_locality) DB_SECS = make_db_partitions_2(SETTINGS);
   else DB_SECS = make_db_partitions_1(SETTINGS, PARTITIONER0);

   final JavaRDD<Long> DB_SIZES =
   DB_SECS.map(item -> BLAST_LIB_SINGLETON.get_size(item)).cache();
   final Long total_size = DB_SIZES.reduce((x, y) -> x + y);

   JavaDStream<BLAST_REQUEST> REQ_STREAM = create_source(SETTINGS);
   if (REQ_STREAM != null) {
   final JavaPairDStream<BLAST_PARTITION, BLAST_REQUEST> JOB_STREAM =
   REQ_STREAM
   .transformToPair(rdd -> DB_SECS.cartesian(rdd).partitionBy(PARTITIONER1.getValue()))
   .cache();

   final JavaPairDStream<String, BLAST_HSP_LIST> HSPS =
   perform_prelim_search(JOB_STREAM, SETTINGS);

   final JavaPairDStream<String, Integer> CUTOFF = calculate_cutoff(HSPS, SETTINGS);

   final JavaPairDStream<String, Tuple2<BLAST_HSP_LIST, Integer>> FILTERED_HSPS =
   filter_by_cutoff(HSPS, CUTOFF, PARTITIONER2);

   final JavaPairDStream<String, BLAST_TB_LIST> SEQANNOTS =
   perform_traceback(FILTERED_HSPS, PARTITIONER2, SETTINGS);

   write_traceback(SEQANNOTS, SETTINGS);
   jssc.start();
   System.out.println(String.format("total database size: %,d bytes", total_size));
   jssc.awaitTermination();
   } else System.out.println("invalid source(s)!");
    }
    */
}
