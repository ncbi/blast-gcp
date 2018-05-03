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
import java.util.Set;
import java.util.Iterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import static java.lang.Math.min;

import java.nio.ByteBuffer;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
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
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkEnv;
import org.apache.spark.rdd.RDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.spark.pubsub.PubsubUtils;

class BLAST_DRIVER extends Thread
{
    private final BLAST_SETTINGS settings;
    private final BLAST_YARN_NODES nodes;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;

    public BLAST_DRIVER( final BLAST_SETTINGS settings, final List< String > files_to_transfer )
    {
        this.settings = settings;
        this.nodes = new BLAST_YARN_NODES();    // discovers yarn-nodes

        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( settings.with_dyn_alloc ) );
        conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
        conf.set( "spark.streaming.receiver.maxRate", String.format( "%d", settings.receiver_max_rate ) );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );
        conf.set( "spark.locality.wait", settings.locality_wait );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( settings.spark_log_level );

        // send the given files to all nodes
        for ( String a_file : files_to_transfer )
            sc.addFile( a_file );

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

    private Integer node_count()
    {
        Integer res = nodes.count();
        if ( res == 0 )
            res = settings.num_executors;
        return res;
    }

    /* ===========================================================================================
            create database-partitions
       =========================================================================================== */
    private JavaRDD< BLAST_PARTITION > make_db_partitions_1( Broadcast< BLAST_SETTINGS > SETTINGS,
        Broadcast< BLAST_PARTITIONER0 > PARTITIONER0 )
    {
        final List< Tuple2< Integer, BLAST_PARTITION > > db_list = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
        {
            BLAST_PARTITION part = new BLAST_PARTITION( settings.db_location, settings.db_pattern,
                i, settings.flat_db_layout );
            db_list.add( new Tuple2<>( i, part ) );
        }

        return sc.parallelizePairs( db_list, node_count() ).partitionBy(
            PARTITIONER0.getValue() ).map( item ->
        {
            BLAST_PARTITION part = item._2();

            BLAST_SETTINGS bls = SETTINGS.getValue();
            if ( bls.log_part_prep )
                BLAST_SEND.send( bls, String.format( "preparing %s", part ) );

            return BLAST_LIB_SINGLETON.prepare( part, bls );
        } ).cache();
    }

    private JavaRDD< BLAST_PARTITION > make_db_partitions_2( Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        final List< Tuple2< BLAST_PARTITION, Seq< String > > > part_list = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
        {
            BLAST_PARTITION part = new BLAST_PARTITION( settings.db_location, settings.db_pattern,
                i, settings.flat_db_layout );

            String host = nodes.getHost( part.getPartition( node_count() ) );

            if ( settings.log_pref_loc )
                BLAST_SEND.send( settings, String.format( "adding %s for %s", host, part ) );

            List< String > prefered_loc = new ArrayList<>();            
            prefered_loc.add( host );

            Seq< String > prefered_loc_seq = JavaConversions.asScalaBuffer( prefered_loc ).toSeq();

            part_list.add( new Tuple2<>( part, prefered_loc_seq ) );
        }

        // we transform thes list into a Seq
        final Seq< Tuple2< BLAST_PARTITION, Seq< String > > > temp1 = JavaConversions.asScalaBuffer( part_list ).toSeq();

        // we need the class-tag for twice for conversions
        ClassTag< BLAST_PARTITION > tag = scala.reflect.ClassTag$.MODULE$.apply( BLAST_PARTITION.class );

        // this will distribute the partitions to different worker-nodes
        final RDD< BLAST_PARTITION > temp2 = sc.toSparkContext( sc ).makeRDD( temp1, tag );

        // now we transform it into a JavaRDD and perform a mapping-op to eventuall load the
        // database onto the worker-node ( if it is not already there )
        return JavaRDD.fromRDD( temp2, tag ).map( item ->
        {
            BLAST_SETTINGS bls = SETTINGS.getValue();

            if ( bls.log_part_prep )
                BLAST_SEND.send( bls, String.format( "preparing %s", item ) );

            return BLAST_LIB_SINGLETON.prepare( item, bls );
        } ).cache();
    }

    /* ===========================================================================================
            create source-stream of Strings
       =========================================================================================== */
    private JavaDStream< String > create_socket_stream()
    {
        return jssc.socketTextStream( settings.trigger_host, settings.trigger_port ).cache();
    }

    private JavaDStream< String > create_pubsub_stream()
    {
        final DStream< PubsubMessage > pub1 = PubsubUtils.createStream( jssc.ssc(),
            settings.project_id, settings.subscript_id );

        final JavaDStream< PubsubMessage > pub2 = JavaDStream$.MODULE$.fromDStream( pub1,
            scala.reflect.ClassTag$.MODULE$.apply( PubsubMessage.class ) );

        return pub2.map( item -> item.getData().toStringUtf8() ).cache();
    }

    private JavaDStream< String > create_file_stream()
    {
        return jssc.textFileStream( settings.hdfs_source_dir ).cache();
    }

    private JavaDStream< BLAST_REQUEST > create_source( Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        JavaDStream< String > tmp = null;

        if ( settings.use_socket_source )
        {
            JavaDStream< String > S_socket = create_socket_stream();
            if ( settings.use_pubsub_source )
            {
                JavaDStream< String > S_pubsub = create_pubsub_stream();
                if ( settings.use_hdfs_source )
                    tmp = S_socket.union( S_pubsub ).union( create_file_stream() ).cache();
                else
                    tmp = S_socket.union( S_pubsub ).cache();
            }
            else
            {
                if ( settings.use_hdfs_source )
                    tmp = S_socket.union( create_file_stream() ).cache();
                else
                    tmp = S_socket.cache();
            }
        }
        else
        {
            if ( settings.use_pubsub_source )
            {
                JavaDStream< String > S_pubsub = create_pubsub_stream();
                if ( settings.use_hdfs_source )
                    tmp = S_pubsub.union( create_file_stream() ).cache();
                else
                    tmp = S_pubsub.cache();
            }
            else
            {
                if ( settings.use_hdfs_source )
                    tmp = create_file_stream().cache();
            }
        }

        if ( tmp != null )
        {
            return tmp.map( item ->
            {
                BLAST_SETTINGS bls = SETTINGS.getValue();

                BLAST_REQUEST request = BLAST_REQUEST_READER.parse( item, bls.top_n );

                if ( bls.log_request )
                    BLAST_SEND.send( bls, String.format( "REQ: '%s'", request.id ) );

                String gs_status_key = String.format( bls.gs_status_file, request.id );
                BLAST_GS_UPLOADER.upload( bls.gs_status_bucket, gs_status_key, bls.gs_status_running );

                return request;
            }).cache();
        }
        return null;
    }


    /* ===========================================================================================
            perform prelim search

            IN  :   SRC: JavaPairDStream < BLAST_PARTITION, BLAST_REQUEST >
            OUT :   JavaPairDStream < STRING: 'PARTITION.NR REQ.ID', BLAST_HSP_LIST >
       =========================================================================================== */
    private void prelim_search_and_traceback( final JavaPairDStream< BLAST_PARTITION, BLAST_REQUEST > SRC,
                                              Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        SRC.foreachRDD( rdd ->
        {
            long count = rdd.count();
            if ( count > 0 )
            {
                rdd.foreachPartition( iter ->
                {
                    BLAST_SETTINGS bls = SETTINGS.getValue();

                    while( iter.hasNext() )
                    {
                        long startTime = System.currentTimeMillis();

                        Tuple2< BLAST_PARTITION, BLAST_REQUEST > item = iter.next();
                        BLAST_PARTITION part = item._1();
                        BLAST_REQUEST req = item._2();

                        List< BLAST_TB_LIST > tb_list = new ArrayList<>();

                        // see if we are at a different worker-id now
                        if ( bls.log_worker_shift )
                        {
                            String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                            if ( !curr_worker_name.equals( part.worker_name ) )
                            {
                                BLAST_SEND.send( bls,
                                                 String.format( "pre worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
                            }
                        }

                        BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( part, bls );
                        if ( blaster != null )
                        {
                            try
                            {
                                BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req, bls.jni_log_level );
                                try
                                {
                                    BLAST_TB_LIST [] tb_results = blaster.jni_traceback( search_res, part, req, bls.jni_log_level );
                                    for ( BLAST_TB_LIST tbl : tb_results )
                                        tb_list.add( tbl );
                                }
                                catch ( Exception e )
                                {
                                    BLAST_SEND.send( bls,
                                                     String.format( "traceback: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
                                }
                            }
                            catch ( Exception e )
                            {
                                BLAST_SEND.send( bls,
                                                 String.format( "prelim-search: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
                            }

                            byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
                            byte[] seq_annot_suffix = { 0, 0, 0, 0, 0, 0, 0, 0 };
                            ByteBuffer buff;

                            if ( tb_list.isEmpty() )
                            {
                                buff = ByteBuffer.allocate( seq_annot_prefix.length + seq_annot_suffix.length );
                                buff.put( seq_annot_prefix );
                                buff.put( seq_annot_suffix );
                            }
                            else
                            {
                                Collections.sort( tb_list );
                                Integer top_n = tb_list.get( 0 ).req.top_n;

                                List< BLAST_TB_LIST > top_n_seq_annots = tb_list.subList( 0, min( tb_list.size(), top_n ) );
                                int sum = 0;
                                for ( BLAST_TB_LIST e : top_n_seq_annots )
                                    sum += e.asn1_blob.length;

                                buff = ByteBuffer.allocate( sum + seq_annot_prefix.length + seq_annot_suffix.length );

                                buff.put( seq_annot_prefix );
                                for ( BLAST_TB_LIST e : top_n_seq_annots )
                                    buff.put( e.asn1_blob );
                                buff.put( seq_annot_suffix );
                            }

                            String id = String.format( "REQ[%s]_PART[%s]", req.id, part.nr ); 
                            if ( bls.gs_or_hdfs.contains( "hdfs" ) )
                            {
                                String fn = String.format( bls.hdfs_result_file, id );
                                String path = String.format( "%s/%s", bls.hdfs_result_dir, fn );
                                Integer uploaded = BLAST_HADOOP_UPLOADER.upload( path, buff );

                                if ( bls.log_final )
                                {
                                    long elapsed = System.currentTimeMillis() - startTime;
                                    BLAST_SEND.send( bls, String.format( "%d bytes written to hdfs at '%s' %,d ms",
                                            uploaded, path, elapsed ) );
                                }
                            }
                            if ( bls.gs_or_hdfs.contains( "gs" ) )
                            {
                                String gs_result_key = String.format( bls.gs_result_file, id );
                                Integer uploaded = BLAST_GS_UPLOADER.upload( bls.gs_result_bucket, gs_result_key, buff );

                                if ( bls.log_final )
                                {
                                    long elapsed = System.currentTimeMillis() - startTime;
                                    BLAST_SEND.send( bls, String.format( "%d bytes written to gs '%s':'%s' %,d ms",
                                            uploaded, bls.gs_result_bucket, gs_result_key, elapsed ) );
                                }
                            }
                        }
                    }
                } );
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
            Broadcast< BLAST_SETTINGS > SETTINGS = sc.broadcast( settings );
            Broadcast< BLAST_PARTITIONER0 > PARTITIONER0 = sc.broadcast( new BLAST_PARTITIONER0( node_count() ) );

            /* ===========================================================================================
                    create database-sections as a static RDD
               =========================================================================================== */
            JavaRDD< BLAST_PARTITION > DB_SECS;
            if ( settings.with_locality )
                DB_SECS = make_db_partitions_2( SETTINGS );
            else
                DB_SECS = make_db_partitions_1( SETTINGS, PARTITIONER0 );

            final JavaRDD< Long > DB_SIZES = DB_SECS.map( item -> BLAST_LIB_SINGLETON.get_size( item ) ).cache();
            final Long total_size = DB_SIZES.reduce( ( x, y ) -> x + y );

            /* ===========================================================================================
                    initialize data-source ( socket as a stand-in for pub-sub )
               =========================================================================================== */
            JavaDStream< BLAST_REQUEST > REQ_STREAM = create_source( SETTINGS );
            if ( REQ_STREAM != null )
            {
                /* ===========================================================================================
                        join request-line from data-source with database-sections
                   =========================================================================================== */
                final JavaPairDStream< BLAST_PARTITION, BLAST_REQUEST > JOB_STREAM
                    = REQ_STREAM.transformToPair( rdd -> DB_SECS.cartesian( rdd ) );

                /* ===========================================================================================
                        perform prelim_search + traceback + final cutoff + writeout for each
                        intersection of PARTITION and REQUEST
                   =========================================================================================== */
                prelim_search_and_traceback( JOB_STREAM, SETTINGS );

                /* ===========================================================================================
                        start the streaming
                   =========================================================================================== */
                jssc.start();
                System.out.println( String.format( "total database size: %,d bytes", total_size ) );
                System.out.println( "driver started..." );
                jssc.awaitTermination();
            }
            else
                System.out.println( "invalid source(s)!" );
        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}
