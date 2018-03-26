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

import java.util.*;
import java.io.*;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkFiles;

class GCP_BLAST_DRIVER extends Thread
{
    private final GCP_BLAST_SETTINGS settings;
    private JavaStreamingContext jssc;

    public GCP_BLAST_DRIVER( final GCP_BLAST_SETTINGS settings )
    {
        this.settings = settings;
    }

    public void stop_blast()
    {
        try
        {
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
            List< GCP_BLAST_PARTITION > partitions = new ArrayList<>();
            for ( int i = 1; i < settings.num_db_partitions; i++ )
            {
                String part_name;
                if ( i < 10 )
                    part_name = String.format( "nt_50M.%02d", i );
                else
                    part_name = String.format( "nt_50M.%d", i );
                partitions.add( new GCP_BLAST_PARTITION( part_name, i ) );
            }
            
            // we broadcast this list to all nodes
            Broadcast< List< GCP_BLAST_PARTITION > > PARTITIONS = jssc.sparkContext().broadcast( partitions );
            Broadcast< String > BUCKET          = jssc.sparkContext().broadcast( settings.bucket );
            Broadcast< String > LOG_HOST        = jssc.sparkContext().broadcast( settings.log_host );
            Broadcast< Integer > LOG_PORT       = jssc.sparkContext().broadcast( settings.log_port );
            Broadcast< String > SAVE_DIR        = jssc.sparkContext().broadcast( settings.save_dir );
            Broadcast< Boolean > LOG_REQUEST    = jssc.sparkContext().broadcast( settings.log_request );
            Broadcast< Boolean > LOG_JOB_START  = jssc.sparkContext().broadcast( settings.log_job_start );
            Broadcast< Boolean > LOG_JOB_DONE   = jssc.sparkContext().broadcast( settings.log_job_done );
            Broadcast< Boolean > LOG_FINAL      = jssc.sparkContext().broadcast( settings.log_final );
            
            JavaDStream< String > LINES = jssc.socketTextStream( settings.trigger_host, settings.trigger_port );
            
            // persist in memory --- prevent recomputing
            LINES.cache();
            
            // create jobs from a request, a request comes in via the socket as 'job_id:db:query:params'
            JavaDStream< GCP_BLAST_JOB > JOBS = LINES.flatMap( line ->
            {
                if ( LOG_REQUEST.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "Request: %s received", line ) );
                
                ArrayList< GCP_BLAST_JOB > tmp = new ArrayList<>();
                GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( line );
                for ( GCP_BLAST_PARTITION partition : PARTITIONS.getValue() )
                    tmp.add( new GCP_BLAST_JOB( req, partition ) );
                return tmp.iterator();
            } );

            // persist in memory --- prevent recomputing
            JOBS.cache();
            
            // repartition with exactly n RDD-partition in each RDD of the Stream
            JavaDStream< GCP_BLAST_JOB > REPARTITIONED_JOBS = JOBS.repartition( settings.num_job_partitions );
            
            // persist in memory --- prevent recomputing
            REPARTITIONED_JOBS.cache();
            
            // send it to the search-function, which turns it into HSP's
            JavaDStream< GCP_BLAST_HSP > SEARCH_RES = REPARTITIONED_JOBS.flatMap( job ->
            {
                ArrayList< GCP_BLAST_HSP > res = new ArrayList<>();

                BlastJNI blaster = new BlastJNI();
                // ++++++ this is the where the work happens on the worker-nodes ++++++

                if ( LOG_JOB_START.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                     String.format( "starting request: '%s' at '%s' ", job.req.req_id, job.partition.name ) );

                Integer count = 0;
                try
                {
                    String[] search_res = blaster.jni_prelim_search( BUCKET.getValue(),
                                                job.req.db, job.req.req_id, job.req.query, job.partition.name, job.req.params );

                    count = search_res.length;
                
                    if ( LOG_JOB_DONE.getValue() )
                        GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                         String.format( "request '%s'.'%s' done -> count = %d", job.req.req_id, job.partition.name, count ) );

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
                                         String.format( "request exeption: '%s' for '%s'", e, job.req.toString() ) );
                }

                if ( count == 0 )
                   res.add( new GCP_BLAST_HSP( job ) ); // empty job
                
                return res.iterator();
            } );

            // persist in memory --- prevent recomputing
            SEARCH_RES.cache();
            
            // filter SEARCH_RES by min_score into FILTERED ( mocked filtering by score, should by take top N higher than score )
            /*
            JavaDStream< GCP_BLAST_HSP > FILTERED = SEARCH_RES.filter( hsp ->
            {
                 return ( hsp.score >= hsp.job.req.min_score );
            } );
           */

            // map FILTERED via simulated Backtrace into FINAL ( mocked by calling toString )
            JavaDStream< String > FINAL = SEARCH_RES.map( hsp -> hsp.toString() );
            FINAL.cache();
            
            // print the FINAL ( this runs on a workernode! )
            FINAL.foreachRDD( rdd -> {
                long count = rdd.count();
                if ( count > 0 )
                {
                    rdd.saveAsTextFile( SAVE_DIR.getValue() );
                    if ( LOG_FINAL.getValue() )
                    {
                        rdd.foreachPartition( rdd_part -> {
                            int i = 0;
                            while( rdd_part.hasNext() && ( i < 10 ) )
                            {
                                GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                                     String.format( "[ %d of %d ] %s", i, count, rdd_part.next() ) );
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
            //System.out.println( "stream_version() exception: " + e );
        }
    }

    @Override public void run()
    {
        try
        {
            SparkConf conf = new SparkConf();
            conf.setAppName( settings.appName );
            conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );

            JavaSparkContext sc = new JavaSparkContext( conf );
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
