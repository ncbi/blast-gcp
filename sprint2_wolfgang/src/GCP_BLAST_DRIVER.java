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

import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;


class GCP_BLAST_DRIVER extends Thread
{
    private final GCP_BLAST_SETTINGS settings;
    private JavaSparkContext sc;
    private JavaStreamingContext jssc;

    public GCP_BLAST_DRIVER( final GCP_BLAST_SETTINGS settings )
    {
        this.settings = settings;
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

    private void stream_version()
    {
        try
        {
            // create a list with N chunks for the database nt04, to be used later for creating jobs out of a request
            List< Tuple2< Integer, GCP_BLAST_PARTITION > > partition_list = new ArrayList<>();
            Integer per_worker = settings.num_db_partitions / settings.num_workers;
            int key = 0;
            for ( int i = 0; i < settings.num_db_partitions; i++ )
            {
                partition_list.add( new Tuple2<>( key, new GCP_BLAST_PARTITION( settings.db_location, settings.db_pattern, i ) ) );
                if ( i % per_worker == 0 ) key++;
            }

            JavaPairRDD< Integer, GCP_BLAST_PARTITION > PARTITIONS = sc.parallelizePairs( partition_list );
            PARTITIONS.persist( StorageLevel.MEMORY_ONLY() );
            
            // we broadcast this list to all nodes
            Broadcast< Integer > NUM_WORKERS    = jssc.sparkContext().broadcast( settings.num_workers );
            Broadcast< String > LOG_HOST        = jssc.sparkContext().broadcast( settings.log_host );
            Broadcast< Integer > LOG_PORT       = jssc.sparkContext().broadcast( settings.log_port );
            Broadcast< String > SAVE_DIR        = jssc.sparkContext().broadcast( settings.save_dir );
            Broadcast< Boolean > LOG_REQUEST    = jssc.sparkContext().broadcast( settings.log_request );
            Broadcast< Boolean > LOG_JOB_START  = jssc.sparkContext().broadcast( settings.log_job_start );
            Broadcast< Boolean > LOG_JOB_DONE   = jssc.sparkContext().broadcast( settings.log_job_done );
            Broadcast< Boolean > LOG_FINAL      = jssc.sparkContext().broadcast( settings.log_final );

            // receive one or multiple lines from the source
            JavaDStream< String >INPUT_STREAM = jssc.socketTextStream( settings.trigger_host, settings.trigger_port );
            INPUT_STREAM.cache();

            // transform the source-lines into a pair-stream consisting of an worker-id and the line
            JavaPairDStream< Integer, String > PAIRED_INPUT_STREAM = INPUT_STREAM.flatMapToPair( line ->
            {
                if ( LOG_REQUEST.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "Request: %s received (1)", line ) );

                List< Tuple2< Integer, String > > tmp = new ArrayList<>();
                for ( int i = 0; i < NUM_WORKERS.getValue(); i++ )
                    tmp.add( new Tuple2<>( i, line ) );
                return tmp.iterator();
            } );
            PAIRED_INPUT_STREAM.cache();

            // join the PARTITIONS with the PAIRED_LINES, to trigger reshuffling
            JavaPairDStream< Integer, Tuple2< String, Optional< GCP_BLAST_PARTITION > > > JOINED_PARTITIONS = PAIRED_INPUT_STREAM.transformToPair( rdd ->
            {
                return rdd.leftOuterJoin( PARTITIONS, NUM_WORKERS.getValue() );
            } );
            JOINED_PARTITIONS.cache();

            // create jobs from a request, a request comes in via the socket as 'job_id:db:query:params'
            JavaDStream< GCP_BLAST_JOB > JOBS = JOINED_PARTITIONS.map( j ->
            {
                Integer jkey                = j._1();
                String req_line             = j._2()._1();
                GCP_BLAST_PARTITION part    = j._2()._2().get();
                if ( LOG_REQUEST.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "Request: %s received (%d)(%s)", req_line, jkey, part.toString() ) );
                
                GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( req_line );
                return new GCP_BLAST_JOB( req, part );
            } );

            // persist in memory --- prevent recomputing
            JOBS.cache();
            
            // send it to the search-function, which turns it into HSP's
            JavaDStream< GCP_BLAST_HSP > SEARCH_RES = JOBS.flatMap( job ->
            {
                /*
                ArrayList< GCP_BLAST_HSP > res = new ArrayList<>();

                BlastJNI blaster = new BlastJNI ();
                // ++++++ this is the where the work happens on the worker-nodes ++++++

                if ( LOG_JOB_START.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                     String.format( "starting request: '%s' at '%s' ", job.req.req_id, job.partition.db_spec ) );

                Integer count = 0;
                try
                {
                    //query, db_spec, program, params 
                    String[] search_res = blaster.jni_prelim_search( job.req.query, job.partition.db_spec, job.req.program, job.req.params );

                    count = search_res.length;
                
                    if ( LOG_JOB_DONE.getValue() )
                        GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                         String.format( "request '%s'.'%s' done -> count = %d", job.req.req_id, job.partition.db_spec, count ) );

                    if ( count > 0 )
                    {
                        for ( String S : search_res )
                        {
                            //GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "HSP: '%s'", S ) );
                            res.add( new GCP_BLAST_HSP( job, S ) );
                        }
                    }
                }
                catch ( Exception e )
                {
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                         String.format( "request exeption: '%s' for '%s'", e, job.toString() ) );
                }

                if ( count == 0 )
                   res.add( new GCP_BLAST_HSP( job ) ); // empty job
                */
                
                GCP_BLAST_JNI_EMULATOR emu = new GCP_BLAST_JNI_EMULATOR();
                ArrayList< GCP_BLAST_HSP > res = emu.make_hsp( job, 3, 10101L );

                if ( LOG_JOB_DONE.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                         String.format( "emu.done : '%s'", job.toString() ) );

                return res.iterator();
            } );

            // persist in memory --- prevent recomputing
            SEARCH_RES.cache();
            
            // map FILTERED via simulated Backtrace into FINAL ( mocked by calling toString )
            JavaDStream< String > FINAL = SEARCH_RES.map( hsp -> hsp.toString() );
            FINAL.cache();
            
            // print the FINAL ( this runs on a workernode! )
            FINAL.foreachRDD( rdd -> {
                long count = rdd.count();
                if ( count > 0 )
                {
                    //rdd.saveAsTextFile( SAVE_DIR.getValue() );
                    if ( LOG_FINAL.getValue() )
                    {
                        rdd.foreachPartition( rdd_part -> {
                            int i = 0;
                            while( rdd_part.hasNext() /* && ( i < 10 ) */ )
                            {
                                GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                                     String.format( "[%d of %d] %s", i, count, rdd_part.next() ) );
                                i += 1;
                            }
                        } );
                    }
                }
            } );

           jssc.start();               // Start the computation
           System.out.println( "driver started..." );
           jssc.awaitTermination();    // Wait for the computation to terminate
        }
        catch ( Exception e )
        {
            System.out.println( "stream_version() exception: " + e );
        }
    }

    @Override public void run()
    {
        try
        {
            SparkConf conf = new SparkConf();
            conf.setAppName( settings.appName );
            conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );

            sc = new JavaSparkContext( conf );
            sc.setLogLevel( "ERROR" );

            // send the given files to all nodes
            for ( String a_file : settings.files_to_transfer )
                sc.addFile( a_file );

            // create a streaming-context from SparkContext given
            jssc = new JavaStreamingContext( sc, Durations.seconds( settings.batch_duration ) );

            stream_version();
        }
        catch ( Exception e )
        {
            System.out.println( "SparkConf exception: " + e );
        }
    }
}
