/*
 *                            PUBLIC DOMAIN NOTICE
 *               National Center for Biotechnology Information
 *
 *  This software/database is a "United States Government Work" under the;
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
 */

//package gov.nih.nlm.ncbi.blastjni;

import gov.nih.nlm.ncbi.blastjni.BlastJNI;
import java.lang.String;
import java.lang.System;
import java.io.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

//import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;


public final class BlastSpark {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: BlastSpark <path_to_jsonl>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("BlastSpark")
            .getOrCreate();

        System.out.println("Spark Session created");

        Random rand=new Random();
        int fake_appid=rand.nextInt(1000000);

        // One per line, set multiline=true for opposite
        Dataset<Row> dsquery=spark.read().format("json").load(args[0]);
        dsquery.printSchema();
        dsquery.show();

        //Dataset<Row> dsdb_partitions=spark.read().format("json").load("db_partitions.jsonl");
        Dataset<Row> dsdb_partitions=spark.read().format("json").load("gs://blastgcp-pipeline-test/db_partitions.jsonl");
        dsdb_partitions.printSchema();
        dsdb_partitions.show();
        //        query.select("select ..."
        long query_cnt=dsquery.count();
        System.out.println("Found " + query_cnt + " query rows");
        long db_partitions_cnt=dsdb_partitions.count();
        System.out.println("Found " + db_partitions_cnt + " db_partition rows");

        dsquery.write().format("json").mode("overwrite").save("/user/vartanianmh/queryout.jsonl");

        Dataset<Row> dsjobs=dsquery.join(dsdb_partitions, "DB").
            repartition(2000);
        long jobs_cnt=dsjobs.count();
        System.out.println("Found " + jobs_cnt + " jobs rows");
        dsjobs.show();
        //System.out.println("jobs has " + dsjobs.parittions.size());


        Dataset<String> jobsjson=dsjobs.toJSON();
        System.out.println("jobsjson is");
        jobsjson.show();
        /*
           Dataset<Row> dsmapped=dsjobs.flatMap(
           (FlatMapFunction<ArrayList<String>, String[]>)
           x-> { return;}, Encoders.STRING());
           */
        System.out.println("Converting to RDD");
        JavaRDD<Row> rddjobs=dsjobs.toJavaRDD();
        System.out.println("Converted to RDD");
        //rddjobs.collect();

        // RID, query, db_part, params
        System.out.println("making arraylist");
        JavaRDD<ArrayList<String>> rddjobs4;
        rddjobs4=rddjobs.map(new Function<Row, ArrayList<String>>() {
            public ArrayList<String> call(Row row) {
                ArrayList<String> l=new ArrayList<String>(4);
                l.add(row.getString(1)); // RID
                l.add(row.getString(2)); // Params
                l.add(row.getString(4)); // partition
                l.add(row.getString(3)); // Query
                return l;
            }});
        System.out.println("made arraylist");

        System.out.println("making rddcsv");
        JavaRDD<String> rddcsv;
        rddcsv=rddjobs.map(new Function<Row, String>() {
            public String call(Row row) {
                return 
                    row.getString(1) +"," + // RID
                    row.getString(2) +"," + // Params
                    row.getString(4) + ","+ // partition
                    row.getString(3); // Query
            }});
        System.out.println("made rddcsv");

        System.out.println("FlatMap");
        JavaRDD<String> rddmap;
        rddmap=rddcsv.flatMap( s-> {
            List<String> csv=Arrays.asList(s.split(","));
            String rid=csv.get(0);
            String params=csv.get(1);
            String db_part=csv.get(2);
            String query=csv.get(3);
            String newresults[] = new BlastJNI().jni_prelim_search(rid,query,db_part,params);
            ArrayList<String> results=new ArrayList<>();
            Collections.addAll(results, newresults);
            //            results=Arrays.asList(newresults);
            return results.iterator();

            //            return results.iterator();
        });

        JavaRDD<String> rddhsp=rddmap.cache().coalesce(1);
        System.out.println("FlatMapped:" + rddhsp.count());

        List<String> lout=rddhsp.collect();
        System.out.println("HSP");
        PrintWriter pw = new PrintWriter(new FileOutputStream(
                    new File("/tmp/hsp" + fake_appid+ ".jsonl"),true));

        long rowcount=0;
        for (String element : lout)
        {
            ++rowcount;
            if (rowcount < 10)
            {
                System.out.println(lout);
            } else if (rowcount==10)
            {
                System.out.println("...");
            }
            pw.println(lout);
        }
        pw.close();

        //s -> Arrays.asList(function(s).iterator()));
        //                               (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
        //                               Encoders.STRING());

        //        JavaRDD<
        /*
           System.out.println("making String[]");
           JavaRDD<String[]> rddjobs5;
           rddjobs5=rddjobs.map(new Function<Row, String[]>() {
           public String[] call(Row row) {
           String[] l=new String[4];
           l[0]=row.getString(0);
           l[1]=row.getString(1);
           l[2]=row.getString(2);
           l[3]=row.getString(3);
           System.out.println("l is " + l);
           return l;
           }});
           System.out.println("made String[]" + rddjobs5.count());
           */
        //JavaRDD<ArrayList<String>> fm=rddjobs4.collect();


        //        rddjobs.saveAsTextFile("job" + r);
        //rddcsv.saveAsTextFile("csv" + fake_appid);
        //rddmap.saveAsTextFile("map" + fake_appid);
        String gscbucket=
            "gs://blastgcp-pipeline-test/output/";
        String filename=gscbucket+"hsp"+fake_appid+".jsonl";
        System.out.println("saving to " + filename);
        rddhsp.saveAsTextFile(filename);

        spark.stop();
    }
}

