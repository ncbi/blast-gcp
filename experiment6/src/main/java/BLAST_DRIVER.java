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
import java.util.concurrent.ConcurrentLinkedQueue;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;

class BLAST_DRIVER extends Thread
{
    private final BLAST_SETTINGS settings;
    private final BLAST_YARN_NODES nodes;
    private final JavaSparkContext sc;
    private Broadcast< BLAST_SETTINGS > SETTINGS;
    private JavaRDD< BLAST_PARTITION > DB_SECS;

    private final ConcurrentLinkedQueue< BLAST_REQUEST > requests;
    private final ConcurrentLinkedQueue< String > cmd;

    public BLAST_DRIVER( final BLAST_SETTINGS settings,
                         final ConcurrentLinkedQueue< BLAST_REQUEST > a_requests,
                         final ConcurrentLinkedQueue< String > a_cmd )
    {
        this.settings = settings;
        this.nodes = new BLAST_YARN_NODES();    // discovers yarn-nodes
        this.requests = a_requests;
        this.cmd = a_cmd;

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
        conf.set( "spark.shuffle.reduceLocality.enabled", Boolean.toString( settings.shuffle_reduceLocality_enabled ) );
        if ( settings.scheduler_fair )
            conf.set( "spark.scheduler.mode", "FAIR" );

        conf.set( "spark.scheduler.allocation.file", "./pooles.xml" );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( settings.spark_log_level );
        sc.addFile( "libblastjni.so" );

        /* ===========================================================================================
                broadcast settings
           =========================================================================================== */
        SETTINGS = sc.broadcast( settings );
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
    private JavaRDD< BLAST_PARTITION > make_db_partitions_1( Broadcast< BLAST_SETTINGS > SE )
    {
        final List< Tuple2< Integer, BLAST_PARTITION > > db_list = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
        {
            BLAST_PARTITION part = new BLAST_PARTITION( settings.db_location, settings.db_pattern,
                i, settings.flat_db_layout );
            db_list.add( new Tuple2<>( i, part ) );
        }

        BLAST_PARTITIONER0 p = new BLAST_PARTITIONER0( settings.num_db_partitions );
        return sc.parallelizePairs( db_list, settings.num_db_partitions ).partitionBy( p ).map( item ->
        {
            BLAST_PARTITION part = item._2();

            BLAST_SETTINGS bls = SE.getValue();
            if ( bls.log_part_prep )
                BLAST_SEND.send( bls, String.format( "preparing %s", part ) );

            return BLAST_LIB_SINGLETON.prepare( part, bls );
        } ).cache();
    }

    private JavaRDD< BLAST_PARTITION > make_db_partitions_2( Broadcast< BLAST_SETTINGS > SE )
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
            BLAST_SETTINGS bls = SE.getValue();

            if ( bls.log_part_prep )
                BLAST_SEND.send( bls, String.format( "preparing %s", item ) );

            return BLAST_LIB_SINGLETON.prepare( item, bls );
        } ).cache();
    }

    @Override public void run()
    {
        try
        {
            /* ===========================================================================================
                    create database-sections as a static RDD
               =========================================================================================== */
            if ( settings.with_locality )
                DB_SECS = make_db_partitions_2( SETTINGS );
            else
                DB_SECS = make_db_partitions_1( SETTINGS );

            final JavaRDD< Long > DB_SIZES = DB_SECS.map( item -> BLAST_LIB_SINGLETON.get_size( item ) ).cache();
            final Long total_size = DB_SIZES.reduce( ( x, y ) -> x + y );

            System.out.println( String.format( "total database size: %,d bytes", total_size ) );
            System.out.println( "driver started..." );

            List< BLAST_JOB > jobs = new ArrayList<>();
            for ( int i = 0; i < settings.parallel_jobs; ++i )
                jobs.add( new BLAST_JOB( settings, sc, SETTINGS, DB_SECS, requests ) );
            for ( BLAST_JOB j : jobs )
                j.start();

            Boolean running = true;
            while( running )
            {
                String line = cmd.poll();
                if ( line != null )
                {
                    if ( line.startsWith( "exit" ) )
                    {
                        running = false;
                    }
                }
                else
                {
                    try
                    {
                        Thread.sleep( 500 );
                    }
                    catch ( InterruptedException e )
                    {
                    }
                }
            }

            for ( BLAST_JOB j : jobs )
                j.signal_done();

            for ( BLAST_JOB j : jobs )
                j.join();

            System.out.println( "driver done..." );
        }
        catch ( Exception e )
        {
            System.out.println( "driver Spark exception: " + e );
        }
    }
}
