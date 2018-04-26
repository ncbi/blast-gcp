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

package gov.nih.nlm.ncbi.exp1;

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

class EXP_DRIVER extends Thread
{
    private final EXP_SETTINGS settings;
    private final EXP_YARN_NODES yarn_nodes;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;

    public EXP_DRIVER( final EXP_SETTINGS settings )
    {
        this.settings = settings;
        this.yarn_nodes = new EXP_YARN_NODES();    // discovers yarn-nodes

        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( settings.with_dyn_alloc ) );
        conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );
        conf.set( "spark.locality.wait", settings.locality_wait );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( "ERROR" );

        // create a streaming-context from SparkContext given
        jssc = new JavaStreamingContext( sc, Durations.seconds( settings.batch_duration ) );
    }

    public void stop_driver()
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

    /* ===========================================================================================
            create partition-rdd
       =========================================================================================== */
    private Integer node_count()
    {
        Integer res = yarn_nodes.count();
        if ( res == 0 )
            res = settings.num_executors;
        return res;
    }

    private JavaRDD< String > make_partitions( Broadcast< EXP_SETTINGS > SETTINGS )
    {
        final List< Tuple2< String, Seq< String > > > part_list = new ArrayList<>();
        for ( int i = 0; i < settings.num_partitions; i++ )
        {
            Integer idx = i % node_count();

            String part = String.format( "PART#%d(%d)", i, idx );
            String host = yarn_nodes.getHost( idx );

            if ( settings.log_pref_loc )
                EXP_SEND.send( settings, String.format( "adding %s for %s", host, part ) );

            List< String > prefered_loc = new ArrayList<>();            
            prefered_loc.add( host );

            Seq< String > prefered_loc_seq = JavaConversions.asScalaBuffer( prefered_loc ).toSeq();

            part_list.add( new Tuple2<>( part, prefered_loc_seq ) );
        }

        // we transform thes list into a Seq
        final Seq< Tuple2< String, Seq< String > > > temp1 = JavaConversions.asScalaBuffer( part_list ).toSeq();

        // we need the class-tag for twice for conversions
        ClassTag< String > tag = scala.reflect.ClassTag$.MODULE$.apply( String.class );

        // this will distribute the partitions to different worker-nodes
        final RDD< String > temp2 = sc.toSparkContext( sc ).makeRDD( temp1, tag );

        // now we transform it into a JavaRDD and perform a mapping-op to eventuall load the
        // database onto the worker-node ( if it is not already there )
        return JavaRDD.fromRDD( temp2, tag ).map( item ->
        {
            EXP_SETTINGS se = SETTINGS.getValue();

            if ( se.log_part_prep )
                EXP_SEND.send( se, String.format( "preparing %s", item ) );

            return item;
        } ).cache();
    }

    /* ===========================================================================================
            create source-stream of Strings
       =========================================================================================== */
    private JavaDStream< String > create_socket_stream( Broadcast< EXP_SETTINGS > SETTINGS )
    {
        JavaDStream< String > tmp = jssc.socketTextStream( settings.trigger_host, settings.trigger_port );
        return tmp.map( item ->
        {
            EXP_SETTINGS se = SETTINGS.getValue();

            if ( se.log_request )
                EXP_SEND.send( se, String.format( "REQ: '%s'", item ) );

            return item;
        } ).cache();
    }

    private void consum_stream( JavaDStream< String > STRM, Broadcast< EXP_SETTINGS > SETTINGS )
    {
        STRM.foreachRDD( rdd ->
        {
            long count = rdd.count();
            if ( count > 0 )
            {
                rdd.foreachPartition( iter ->
                {
                    EXP_SETTINGS se = SETTINGS.getValue();
                    while( iter.hasNext() )
                    {
                        String item = iter.next();

                        if ( se.log_final )
                            EXP_SEND.send( se, String.format( "final : '%s'", item ) );
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
            Broadcast< EXP_SETTINGS > SETTINGS = sc.broadcast( settings );
            Broadcast< EXP_YARN_NODES > YARN_NODES = sc.broadcast( yarn_nodes );

            /* ===========================================================================================
                    make PARTS
               =========================================================================================== */
            JavaRDD< String > PARTS = make_partitions( SETTINGS );
            final JavaRDD< Integer > P_SIZES = PARTS.map( item -> item.length() );
            final Integer total_size = P_SIZES.reduce( ( x, y ) -> x + y );


            /* ===========================================================================================
                    initialize data-source ( socket as a stand-in for pub-sub )
               =========================================================================================== */
            final JavaDStream< String > REQ_STREAM = create_socket_stream( SETTINGS );

            final JavaPairDStream< String, String > JOB_STREAM
                = REQ_STREAM.transformToPair( rdd -> PARTS.cartesian( rdd ) ).cache();
            
            final JavaDStream< String > RES_STREAM = JOB_STREAM.map( item ->
            {
                return String.format( "%s-%s", item._1(), item._2() );
            });

            consum_stream( RES_STREAM, SETTINGS );

            /* ===========================================================================================
                    start the streaming
               =========================================================================================== */
            jssc.start();
            System.out.println( String.format( "total parts size: %,d bytes", total_size ) );
            System.out.println( "driver started..." );
            jssc.awaitTermination();

        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}

