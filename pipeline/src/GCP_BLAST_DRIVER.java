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

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;

import scala.Tuple2;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.api.java.JavaDStream$;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.HashPartitioner;

/*
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.spark.pubsub.PubsubUtils;
*/

class GCP_BLAST_DRIVER extends Thread
{
    private final GCP_BLAST_SETTINGS settings;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;
    private final List< GCP_BLAST_PARTITION > db_sec_list;

    public GCP_BLAST_DRIVER( final GCP_BLAST_SETTINGS settings )
    {
        this.settings = settings;

        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );
        conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
        conf.set( "spark.streaming.receiver.maxRate", String.format( "%d", settings.receiver_max_rate ) );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( "ERROR" );

        // send the given files to all nodes
        for ( String a_file : settings.files_to_transfer )
            sc.addFile( a_file );

        db_sec_list = create_db_secs();

        // create a streaming-context from SparkContext given
        jssc = new JavaStreamingContext( sc, Durations.seconds( settings.batch_duration ) );
    }

    public void stop_blast()
    {
        try
        {
            if ( jssc != null )
                jssc.stop( true, true );
        }
        catch ( Exception e )
        {
            System.out.println( "JavaStreamingContext.stop() : " + e );
        }
    }

    private List< GCP_BLAST_PARTITION > create_db_secs()
    {
        List< GCP_BLAST_PARTITION > res = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
            res.add( new GCP_BLAST_PARTITION( settings.db_location, settings.db_pattern, i, settings.flat_db_layout ) );
        return res;
    }


    /* ===========================================================================================
            perform prelim search
       =========================================================================================== */
    private JavaPairDStream< String, GCP_BLAST_HSP_LIST > perform_prelim_search(
            final JavaPairDStream< GCP_BLAST_PARTITION, String > SRC,
            Broadcast< Integer > TOP_N,
            Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT,
            Broadcast< Boolean > LOG_JOB_START, Broadcast< Boolean > LOG_JOB_DONE )
    {
        return SRC.flatMapToPair( item -> {
            ArrayList< Tuple2< String, GCP_BLAST_HSP_LIST > > ret = new ArrayList<>();

            GCP_BLAST_PARTITION part = item._1();
            GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( item._2(), TOP_N.getValue() ); // REQ-LINE to REQUEST

            // ++++++ this is the where the work happens on the worker-nodes ++++++
            if ( LOG_JOB_START.getValue() )
                GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                 String.format( "starting request: '%s' at '%s' ", req.req_id, part.db_spec ) );

            Integer count = 0;
            try
            {
                String rid = req.req_id;

                long startTime = System.currentTimeMillis();
                GCP_BLAST_LIB blaster = new GCP_BLAST_LIB();
                GCP_BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req );
                long elapsed = System.currentTimeMillis() - startTime;

                count = search_res.length;
            
                if ( LOG_JOB_DONE.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                     String.format( "request '%s'.'%s' done -> count = %d", rid, part.db_spec, count ) );

                for ( GCP_BLAST_HSP_LIST S : search_res )
                    ret.add( new Tuple2<>( String.format( "%d %s", part.nr, req.req_id ), S ) );

                /*
                if ( ret.isEmpty() )
                {
                    // if we have no hsps at all, insert an empty one
                    ret.add( new Tuple2<>( String.format( "%d %s", part.nr, req.req_id ),
                                           new GCP_BLAST_HSP_LIST( req, part, elapsed ) ) );
                }
                */
            }
            catch ( Exception e )
            {
                GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                     String.format( "request exeption: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
            }
            return ret.iterator();
        }).cache();
    }


    /* ===========================================================================================
            calculate cutoff
       =========================================================================================== */
    private JavaPairDStream< String, Integer > calculate_cutoff( final JavaPairDStream< String, GCP_BLAST_HSP_LIST > HSPS,
             Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
        // key   : req_id
        // value : scores, each having a copy of N for picking the cutoff value
        final JavaPairDStream< String, GCP_BLAST_RID_SCORE > TEMP1 = HSPS.mapToPair( item -> {
            GCP_BLAST_RID_SCORE ris = new GCP_BLAST_RID_SCORE( item._2().max_score, item._2().requestobj.top_n );
            String key = item._1();
            String req_id = key.substring( key.indexOf( ' ' ), key.length() );
            return new Tuple2< String, GCP_BLAST_RID_SCORE >( req_id, ris );
        }).cache();

        // key   : req_id
        // value : list of scores, each having a copy of N for picking the cutoff value
        // this will conentrate all GCP_BLAST_RID_SCORE instances for a given req_id on one worker node
        final JavaPairDStream< String, Iterable< GCP_BLAST_RID_SCORE > > TEMP2 = TEMP1.groupByKey().cache();

        // key   : req_id
        // value : the cutoff value
        // this will run once for a given req_id on one worker node
        return TEMP2.mapToPair( item -> {
            Integer cutoff = 0;
            Integer top_n = 0;
            ArrayList< Integer > lst = new ArrayList<>();
            for( GCP_BLAST_RID_SCORE s : item._2() )
            {
                if ( top_n == 0 )
                    top_n = s.top_n;
                lst.add( s.score );
            }
            if ( lst.size() > top_n )
            {
                Collections.sort( lst, Collections.reverseOrder() );
                cutoff = lst.get( top_n - 1 );                    
            }
            GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                             String.format( "CUTOFF[ %s ] : %d", item._1(), cutoff ) );
            return new Tuple2<>( item._1(), cutoff );

        }).cache();
    }


    /* ===========================================================================================
            filter by cutoff
       =========================================================================================== */
    private JavaPairDStream< String, Tuple2< GCP_BLAST_HSP_LIST, Integer > > filter_by_cutoff(
                                JavaPairDStream< String, GCP_BLAST_HSP_LIST > HSPS,
                                JavaPairDStream< String, Integer > CUTOFF,
                                Broadcast< GCP_BLAST_CustomPartitioner2 > CUST_PART2,
                                Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
		JavaPairDStream< String, String > ACTIVE_PARTITIONS = 
			HSPS.transform( rdd -> rdd.keys().distinct() )
            .mapToPair( item-> new Tuple2<>( item.substring( item.indexOf( ' ' ), item.length() ), item ) );

        JavaPairDStream< String, Integer > CUTOFF2 = 
			ACTIVE_PARTITIONS.join( CUTOFF ).mapToPair( item -> item._2() );

        return HSPS.join( CUTOFF2, CUST_PART2.getValue() ) . filter( item ->
        {
            GCP_BLAST_HSP_LIST hsps = item._2()._1();
            Integer cutoff = item._2()._2();
            return ( cutoff == 0 || hsps.max_score >= cutoff );
        });
    }


    /* ===========================================================================================
            perform traceback
       =========================================================================================== */
    private JavaDStream< String > perform_traceback( JavaPairDStream< String, Tuple2< GCP_BLAST_HSP_LIST, Integer > > HSPS )
    {
        return HSPS.flatMap( item -> {
            ArrayList< String > ret = new ArrayList<>();

            // this is a proxy for the traceback, right now it just converts a HSP_LIST into a string
            GCP_BLAST_HSP_LIST [] a = new GCP_BLAST_HSP_LIST[ 1 ];
            a[ 0 ] = item._2()._1();

            GCP_BLAST_LIB blaster = new GCP_BLAST_LIB();
            GCP_BLAST_TB_LIST [] results = blaster.jni_traceback( a, a[ 0 ].partitionobj, a[ 0 ].requestobj );
            
            for ( GCP_BLAST_TB_LIST L : results )
                ret.add( L.toString() );

            //return hsps.toString_limit( 5 );
            //return hsps.tabulated();
            return ret.iterator();
        }).cache();
    };


    /* ===========================================================================================
            write results
       =========================================================================================== */
    private void write_results( JavaDStream< String > SEQANNOT,
                                Broadcast< String > SAVE_DIR, Broadcast< Boolean > LOG_FINAL,
                                Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
        SEQANNOT.foreachRDD( rdd -> {
            long count = rdd.count();
            if ( count > 0 )
            {
                if ( LOG_FINAL.getValue() )
                {
                    rdd.foreachPartition( iter -> {
                        while( iter.hasNext() )
                            GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), iter.next() );
                    } );
                }
                GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                String.format( "REQUEST DONE: %d (%d)", count, rdd.id() ) );
                rdd.saveAsTextFile( String.format( "%s%d", SAVE_DIR.getValue(), rdd.id() ) );
            }
        } );
    }

    @Override public void run()
    {
        try
        {
            /* ===========================================================================================
                    broadcast settings
               =========================================================================================== */
            Broadcast< String > LOG_HOST = sc.broadcast( settings.log_host );
            Broadcast< Integer > LOG_PORT = sc.broadcast( settings.log_port );
            Broadcast< String > SAVE_DIR = sc.broadcast( settings.save_dir );
            Broadcast< Boolean > LOG_REQUEST = sc.broadcast( settings.log_request );
            Broadcast< Boolean > LOG_JOB_START = sc.broadcast( settings.log_job_start );
            Broadcast< Boolean > LOG_JOB_DONE = sc.broadcast( settings.log_job_done );
            Broadcast< Boolean > LOG_FINAL = sc.broadcast( settings.log_final );
            Broadcast< Integer > TOP_N = sc.broadcast( settings.top_n );
            Broadcast< GCP_BLAST_CustomPartitioner > CUST_PART = sc.broadcast(
                                            new GCP_BLAST_CustomPartitioner( settings.num_workers ) );
            Broadcast< GCP_BLAST_CustomPartitioner2 > CUST_PART2 = sc.broadcast(
                                            new GCP_BLAST_CustomPartitioner2( settings.num_workers ) );

            /* ===========================================================================================
                    create database-sections as a static RDD
               =========================================================================================== */
            final JavaRDD< GCP_BLAST_PARTITION > DB_SECS = sc.parallelize( db_sec_list ).cache();
			final Integer numbases = DB_SECS.map( bp -> bp.getSize() ).reduce( ( x, y ) -> x + y );

            /* ===========================================================================================
                    initialize data-source ( socket as a stand-in for pub-sub )
               =========================================================================================== */
            /*
            final DStream< PubsubMessage > pub1 = PubsubUtils.createStream( jssc.ssc(),
                settings.project_id, settings.subscript_id );

            final JavaDStream< PubsubMessage > pub2 = JavaDStream$.MODULE$.fromDStream( pub1,
                scala.reflect.ClassTag$.MODULE$.apply( PubsubMessage.class ) );

            final JavaDStream< String > REQ_STREAM = pub2.map( item -> item.getData().toStringUtf8() );
            */

            final JavaDStream< String > REQ_STREAM
                = jssc.socketTextStream( settings.trigger_host, settings.trigger_port ).cache();

            /* ===========================================================================================
                    join request-line from data-source with database-sections
               =========================================================================================== */
            final JavaPairDStream< GCP_BLAST_PARTITION, String > JOINED_REQ_STREAM
                = REQ_STREAM.transformToPair( rdd -> 
                            DB_SECS.cartesian( rdd ).partitionBy( CUST_PART.getValue() ) ).cache();

            /* ===========================================================================================
                    perform prelim search
               =========================================================================================== */
            final JavaPairDStream< String, GCP_BLAST_HSP_LIST > HSPS
                = perform_prelim_search( JOINED_REQ_STREAM, TOP_N, LOG_HOST, LOG_PORT,
                                         LOG_JOB_START, LOG_JOB_DONE );

            /* ===========================================================================================
                    calculate cutoff
               =========================================================================================== */
            final JavaPairDStream< String, Integer > CUTOFF = calculate_cutoff( HSPS, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    filter by cutoff
               =========================================================================================== */
            final JavaPairDStream< String, Tuple2< GCP_BLAST_HSP_LIST, Integer> > FILTERED 
                = filter_by_cutoff( HSPS, CUTOFF, CUST_PART2, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    perform traceback
               =========================================================================================== */
            final JavaDStream< String > SEQANNOT = perform_traceback( FILTERED );

            /* ===========================================================================================
                    write results
               =========================================================================================== */
            write_results( SEQANNOT, SAVE_DIR, LOG_FINAL, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    start the streaming
               =========================================================================================== */
            jssc.start();
            System.out.println( "database size: " + numbases.toString() );
            System.out.println( "driver started..." );
            jssc.awaitTermination();
        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}
