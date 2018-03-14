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

public final class GCP_BLAST
{
    public static void stream_version( JavaSparkContext sc, int num_samples, int min_score )
    {
        try
        {
            // send the file libblastjni.so to all nodes, the java-class BlastJNI will load this lib in it's static initialization
            // via System.load( SparkFiles.get( "libblastjni.so" ) );
            sc.addFile( "libblastjni.so" );
            
            // create a streaming-context from SparkContext given
            JavaStreamingContext jssc = new JavaStreamingContext( sc, Durations.seconds( 1 ) );
            
            // create a list with 200 chunks for the database nt04, to be used later for creating jobs out of a request
            int num_chunks = 200;
            List< GCP_BLAST_CHUNK > chunk_list = new ArrayList<>();
            for ( int i = 0; i < num_chunks; i++ )
                chunk_list.add( new GCP_BLAST_CHUNK( String.format( "nt04_%d", i ), i ) );

            // we broadcast this list to all nodes
            Broadcast< List< GCP_BLAST_CHUNK > > CHUNKS = jssc.sparkContext().broadcast( chunk_list );

            // Create a local StreamingContext listening on port localhost.9999
            JavaReceiverInputDStream< String > lines = jssc.socketTextStream( "localhost", 9999 );
            
            // create jobs from a request, a request comes in via the socket as 'job_id:db:query:params'
            JavaDStream< GCP_BLAST_JOB > JOBS = lines.flatMap( line ->
            {
                ArrayList< GCP_BLAST_JOB > tmp = new ArrayList<>();
                GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( line );
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
                String[] blast_res = blaster.jni_prelim_search( job.req.job_id, job.req.query, job.chunk.name, job.req.params );
                for ( String S : blast_res )
                    res.add( new GCP_BLAST_HSP( job, S ) );

                return res.iterator();
            } );
            
            // filter SEARCH_RES by min_score into FILTERED ( mocked filtering by score, should by take top N higher than score )
            JavaDStream< GCP_BLAST_HSP > FILTERED = SEARCH_RES.filter( hsp -> hsp.score > min_score );

            // map FILTERED via simulated Backtrace into FINAL ( mocked by calling toString )
            JavaDStream< String > FINAL = FILTERED.map( hsp -> hsp.toString() );
            
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
                else
                    System.out.println( "." );
            } );
            
            jssc.start();               // Start the computation
            jssc.awaitTermination();    // Wait for the computation to terminate
        }
        catch ( Exception e )
        {
            System.out.println( "stream_version . exception: " + e );
        }
    }
    
	public static void main( String[] args ) throws Exception
	{
        try
        {
            SparkConf conf = new SparkConf();
            conf.setAppName( GCP_BLAST.class.getSimpleName() );
            conf.setMaster( "local[4]" );
            conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
            
            JavaSparkContext sc = new JavaSparkContext( conf );
            sc.setLogLevel( "ERROR" );
            
            int num_samples = 10;
            int min_score = 300;

            stream_version( sc, num_samples, min_score );
        }
        catch ( Exception e )
        {
            System.out.println( "SparkConf exception: " + e );
        }
	}
}
