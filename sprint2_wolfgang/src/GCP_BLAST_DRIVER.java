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
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.HashPartitioner;

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
            // we broadcast this list to all nodes
            Broadcast< Integer > NUM_WORKERS    = jssc.sparkContext().broadcast( settings.num_workers );
            Broadcast< String > LOG_HOST        = jssc.sparkContext().broadcast( settings.log_host );
            Broadcast< Integer > LOG_PORT       = jssc.sparkContext().broadcast( settings.log_port );
            Broadcast< String > SAVE_DIR        = jssc.sparkContext().broadcast( settings.save_dir );
            Broadcast< Boolean > LOG_REQUEST    = jssc.sparkContext().broadcast( settings.log_request );
            Broadcast< Boolean > LOG_JOB_START  = jssc.sparkContext().broadcast( settings.log_job_start );
            Broadcast< Boolean > LOG_JOB_DONE   = jssc.sparkContext().broadcast( settings.log_job_done );
            Broadcast< Boolean > LOG_FINAL      = jssc.sparkContext().broadcast( settings.log_final );

            // create a list with N chunks for the database nt04, to be used later for creating jobs out of a request
            List< GCP_BLAST_PARTITION > db_sec_list = new ArrayList<>();
            for ( int i = 0; i < settings.num_db_partitions; i++ )
                db_sec_list.add( new GCP_BLAST_PARTITION( settings.db_location, settings.db_pattern, i ) );
            GCP_BLAST_CustomPartitioner myPartitioner = new GCP_BLAST_CustomPartitioner(settings.num_workers);
            JavaRDD<GCP_BLAST_PARTITION > DB_SEC = sc.parallelize( db_sec_list).cache();
			Integer numbases = DB_SEC.map(bp -> bp.getSize()).reduce((x, y) -> x + y);
		

            // receive one or multiple lines from the source
            JavaDStream< String > REQ_STREAM = jssc.socketTextStream( settings.trigger_host, settings.trigger_port );
            REQ_STREAM.cache();

            // join the PARTITIONS with the PAIRED_LINES, to trigger reshuffling
            JavaPairDStream< GCP_BLAST_PARTITION,String> JOINED_REQ_STREAM
                = REQ_STREAM.transformToPair( rdd ->
            {
                return DB_SEC.cartesian( rdd ).partitionBy(myPartitioner);
            } ).repartition( settings.num_workers );
            JOINED_REQ_STREAM.cache();

            // create jobs from a request, a request comes in via the socket as 'job_id:db:query:params'
            JavaPairDStream< String, GCP_BLAST_JOB > JOBS = JOINED_REQ_STREAM.mapToPair( j ->
            {
                String jkey                 = "nt";
                String req_line             = j._2();
                GCP_BLAST_PARTITION part    = j._1();
                if ( LOG_REQUEST.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "Request: %s received (%s)(%s)", req_line, jkey, part.toString() ) );
                
                GCP_BLAST_REQUEST req = new GCP_BLAST_REQUEST( req_line );
                return new Tuple2<>( part.name, new GCP_BLAST_JOB( req, part ) );
            } ).cache();


            //JavaPairDStream< String, GCP_BLAST_JOB > PJOBS = JOBS.repartition( 2 );
            //PJOBS.cache();


            //List< List< Tuple2< String, GCP_BLAST_JOB > > > lPj = PJOBS.glom().collect();
            //GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
            //        String.format( "PJOBS.partitions: %s", lPj ) );

            // send it to the search-function, which turns it into HSP's
            JavaDStream< GCP_BLAST_HSP > SEARCH_RES = JOBS.flatMap( job_pair ->
            {
                GCP_BLAST_JOB job = job_pair._2();
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
                    String[] search_res = blaster.jni_prelim_search( job.req.req_id, job.req.query, job.partition.db_spec, job.req.params );

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

                /*                
                GCP_BLAST_JNI_EMULATOR emu = new GCP_BLAST_JNI_EMULATOR();
                ArrayList< GCP_BLAST_HSP > res = emu.make_hsp( job._2(), 3, 10101L );

                if ( LOG_JOB_DONE.getValue() )
                    GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(),
                                         String.format( "emu.done : '%s'", job._2().toString() ) );
                */
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
                    rdd.saveAsTextFile( SAVE_DIR.getValue() );
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
					GCP_BLAST_SEND.send( LOG_HOST.getValue(), LOG_PORT.getValue(), String.format( "REQUEST DONE: %d", count ) );
                }
            } );

           jssc.start();               // Start the computation
           System.out.println( "database size: " + numbases.toString() );
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
