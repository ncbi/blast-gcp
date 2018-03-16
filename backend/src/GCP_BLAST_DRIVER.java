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
    private final String appName;
    private final String master;
    private final List< String > files_to_transfer;
    private JavaStreamingContext jssc;
    
    public GCP_BLAST_DRIVER( final String appName, final String master, final List< String > files_to_transfer )
    {
        this.appName = appName;
        this.master = master;
        this.files_to_transfer = files_to_transfer;
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
    
    private void stream_version( int dflt_min_score )
    {
        try
        {
            // create a list with N chunks for the database nt04, to be used later for creating jobs out of a request
            int num_chunks = 20;
            List< GCP_BLAST_CHUNK > chunk_list = new ArrayList<>();
            for ( int i = 0; i < num_chunks; i++ )
                chunk_list.add( new GCP_BLAST_CHUNK( String.format( "nt.%02d", i + 1 ), i ) );

            // we broadcast this list to all nodes
            Broadcast< List< GCP_BLAST_CHUNK > > CHUNKS = jssc.sparkContext().broadcast( chunk_list );

            // Create a local StreamingContext listening on port localhost.9999
            JavaReceiverInputDStream< String > lines = jssc.socketTextStream( "localhost", 9999 );
            
            // create jobs from a request, a request comes in via the socket as 'job_id:db:query:params'
            JavaDStream< GCP_BLAST_JOB > JOBS = lines.flatMap( line ->
            {
                System.out.println( String.format( "Request: %s received", line ) );
                ArrayList< GCP_BLAST_JOB > tmp = new ArrayList<>();
                GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( line, dflt_min_score );
                // we are using the broadcasted ArrayList called 'CHUNKS' to create the jobs
                for ( GCP_BLAST_CHUNK chunk : CHUNKS.getValue() )
                    tmp.add( new GCP_BLAST_JOB( req, chunk ) );
                return tmp.iterator();
            } );
            
            // send it to the search-function, which turns it into HSP's
            JavaDStream< GCP_BLAST_HSP > SEARCH_RES = JOBS.flatMap( job ->
            {
                ArrayList< GCP_BLAST_HSP > res = new ArrayList<>();
                
                BlastJNI blaster = new BlastJNI();
                // ++++++ this is the where the work happens on the worker-nodes ++++++
                System.out.println( String.format( "starting request: '%s' at '%s' ", job.req.req_id, job.chunk.name ) );
                String[] blast_res = blaster.jni_prelim_search( job.req.req_id, job.req.query, job.chunk.name, job.req.params );
                Integer count = blast_res.length;
                if ( count > 0 )
                {
                    for ( String S : blast_res )
                        res.add( new GCP_BLAST_HSP( job, S ) );
                }
                return res.iterator();
            } );
            
            // filter SEARCH_RES by min_score into FILTERED ( mocked filtering by score, should by take top N higher than score )
//            JavaDStream< GCP_BLAST_HSP > FILTERED = SEARCH_RES.filter( hsp ->
 //           {
  //              return ( hsp.score >= hsp.job.req.min_score );
   //         } );

            // map FILTERED via simulated Backtrace into FINAL ( mocked by calling toString )
            JavaDStream< String > FINAL = SEARCH_RES.map( hsp -> hsp.toString() );
            
            // print the FINAL ( this runs on a workernode! )
            FINAL.foreachRDD( rdd -> {
                long count = rdd.count();
                if ( count > 0 )
                {
                    System.out.println( String.format( "-------------------------- [%d]", count ) );
                    rdd.foreachPartition( part -> {
                        int i = 0;
                        while( part.hasNext() && ( i < 10 ) )
                        {
                            System.out.println( part.next() );
                            i += 1;
                        }
                    } );
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
            conf.setAppName( this.appName );
//            conf.setMaster( this.master );
            conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
//           conf.set( "spark.yarn.jars", "" );
//            conf.set( "spark.executorEnv.LD_LIBRARY_PATH","/tmp/blast");
            
            JavaSparkContext sc = new JavaSparkContext( conf );
            sc.setLogLevel( "ERROR" );

            // send the given files to all nodes
            for ( String a_file : files_to_transfer )
                sc.addFile( a_file );
            
            // create a streaming-context from SparkContext given
            jssc = new JavaStreamingContext( sc, Durations.seconds( 1 ) );
            
            int dflt_min_score = 1;
            stream_version( dflt_min_score );
        }
        catch ( Exception e )
        {
            System.out.println( "SparkConf exception: " + e );
        }
    }
}
