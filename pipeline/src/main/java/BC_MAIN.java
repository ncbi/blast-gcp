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

import java.io.File;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;


public final class BC_MAIN
{

/**
 * run the Blast-Spark-Application :
 * - create the SparkContext
 * - create instances of infrastructure-classes
 *     ( BC_CONTEXT, BC_CONSOLE, BC_DEBUG_RECEIVER, BC_JOBS )
 * - broadcast the debug-settings to all workers
 * - create and populate the dictionary of database-chunks
 * - parallelize ( send to workers ) the database-chunks
 * - perform a simple map-reduce operation on the database-chunks
 *     to force downloading them to the workers
 * - collect a container of strings reporting the download and
 *     save this report to the file 'downloads.txt' on the master
 * - run in a loop until termination requested ( from commandline )
 * - in the loop handle commands from input like process single
 *     requests, list of requests, buckets of requests, quit
 * - join threads in BC_JOBS, BC_DEBUG_RECEICER, BC_CONSOLE and BC_CONTEXT
 *
 * @param settings  parsed and validated application settings
 * @see        BC_SETTINGS
 * @see        BC_SETTINGS_READER
 * @see        BC_CONTEXT
 * @see        BC_CONSOLE
 * @see        BC_DEBUG_RECEIVER
 * @see        BC_DEBUG_SETTINGS
*/
    private static void run( final BC_SETTINGS settings, final String list_to_run )
    {
        /* create and configure the spark-context */
        SparkConf sc = BC_SETTINGS_READER.createSparkConfAndConfigure( settings );
        JavaSparkContext jsc = new JavaSparkContext( sc );
        jsc.addFile( "libblastjni.so" );
        jsc.setLogLevel( settings.spark_log_level );

        int num_executors = jsc.getConf().getInt( "spark.executor.instances", 0 );
        System.out.println( String.format( "we have %d executors", num_executors ) );
        System.out.println( String.format( "running on Spark-version: '%s'", jsc.sc().version() ) );

        /* create the application-context, lives only on the master */
        BC_CONTEXT context = new BC_CONTEXT( settings );

        // reader thread for console commands
        BC_CONSOLE console = new BC_CONSOLE( context );
        console.start();

        // listen for debug-events
        BC_DEBUG_RECEIVER debug_receiver = null;
        if ( settings.debug.events_selected() )
        {
            debug_receiver = new BC_DEBUG_RECEIVER( context );
            debug_receiver.start();
        }

        /* broadcast the Debug-settings */
        Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS = jsc.broadcast( settings.debug );

        Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict = new HashMap<>();

        /* populate db_dict */
        for ( String key : settings.dbs.keySet() )
        {
            BC_DATABASE_SETTING db_setting = settings.dbs.get( key );

            /* get a list of entries from the source ( bucket ) */
            List< String > files = BC_GCP_TOOLS.list( db_setting.source_location );

            /* get a list of unique names ( without the extension ) */
            List< String > names = BC_GCP_TOOLS.unique_without_extension( files, db_setting.extensions );
            if ( db_setting.limit > 0 )
                System.out.println( String.format( "%s has %d chunks, using %d of them", key, names.size(), db_setting.limit ) );
            else
                System.out.println( String.format( "%s has %d chunks", key, names.size() ) );

            /* create a list of Database-RDD-entries using a static method of this class */
            List< BC_DATABASE_RDD_ENTRY > entries = BC_DATABASE_RDD_ENTRY.make_rdd_entry_list( db_setting,
                db_setting.limit > 0 ? names.subList( 0, db_setting.limit ) : names );

            /* ask the spark-context to distribute the RDD to the workers */
            JavaRDD< BC_DATABASE_RDD_ENTRY > rdd = jsc.parallelize( entries, num_executors * 4 );

            /* put the RDD in the database-dictionary */
            db_dict.put( key, rdd );
        }

        /* create the job-pool to process jobs in parallel */
        BC_JOBS jobs = new BC_JOBS( context, jsc, DEBUG_SETTINGS, db_dict );

        System.out.println( "ready" );

        /* if we have a list to run at startup... */
        if ( list_to_run != null )
            context.addRequestList( list_to_run, System.out, 0 );

        /* we are handling commands, which originate form the master-console or from a socket */
        while ( context.is_running() )
        {
            try
            {
                BC_COMMAND cmd = context.pull_cmd();
                if ( cmd != null )
                    cmd.handle( context );
                else
                    Thread.sleep( 250 );
            }
            catch ( InterruptedException e ) {}
        }

        /* join the different threads we have created... */
        try
        {
            jobs.join();
            if ( debug_receiver != null )
                debug_receiver.join_clients();
            console.join();
            context.join();     /* because context owns request-list-threads */
        }
        catch( Exception e ) { e.printStackTrace(); }

        System.out.println( "done" );
    }

/**
 * Main-method of the Blast-Spark-Application :
 * - reads and parses the settings-file into an instance of BC_SETTINGS
 * - prints the settings
 * - checks if the settings are valid ( nothing essential is missing )
 * - calls the static run-method of this class
 *
 * @param args command-line arguments, 1st arg is name of settings-file
 * @see        BC_SETTINGS
 * @see        BC_SETTINGS_READER
*/
    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
            System.out.println( "settings-file not specified" );
        else
        {
            String settings_file_name = args[ 0 ];
            if ( BC_UTILS.file_exists( settings_file_name ) )
            {
                System.out.println( String.format( "reading settings from file: '%s'", settings_file_name ) );
                /* parse the settings-file */
                BC_SETTINGS settings = BC_SETTINGS_READER.read_from_json( settings_file_name, BC_MAIN.class.getSimpleName() );
                System.out.println( settings );
                if ( !settings.valid() )
                    System.out.println( "settings are invalid!, exiting..." );
                else
                {
                    System.out.println( "settings are valid! preparing cluster" );

                    String list_to_run = args.length > 1 ? args[ 1 ] : null;
                    /* === run the application === */
                    run( settings, list_to_run );
                }
            }
            else
                System.out.println( String.format( "settings-file '%s' not found", settings_file_name ) );
        }
   }
}

