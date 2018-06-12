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

//import java.io.PrintStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public final class BLAST_MAIN
{

    /* the handling of all user-interface ( console as well as socket-communication is centralized here */

    public static String main_spark_loop( final BLAST_STATUS status,
                                          final BLAST_JOBS jobs,
                                          int top_n,
                                          final String ini_file )
    {
        String res = "";
        String cmd;
        BLAST_LIST_SUBMIT submitter = null;

        while( status.is_running() )
        {
            try
            {
                CMD_Q_ENTRY e = status.get_cmd();
                if ( e != null )
                {
                    if ( e.line.startsWith( "J" ) )
                        jobs.set( e.line.substring( 1 ) );
                    else if ( e.line.equals( "exit" ) )
                        status.stop();
                    else if ( e.line.equals( "status" ) )
                        e.stream.printf( "%s\n", status );
                    else if ( e.line.equals( "backlog" ) )
                        e.stream.printf( "%d\n", status.get_backlog() );
                    else if ( e.line.startsWith( "reset" ) )
                    {
                        status.stop();
                        String[] splited = e.line.split( "\\s+" );
                        if ( splited.length > 1 )
                            res = splited[ 1 ];
                        else
                            res = ini_file;
                    }
                    else if ( e.line.startsWith( "R" ) )
                        status.add_request_string( e.line.substring( 1 ), e.stream, top_n );
                    else if ( e.line.startsWith( "F" ) )
                        status.add_request_file( e.line.substring( 1 ), e.stream, top_n );
                    else if ( e.line.startsWith( "L" ) )
                    {
                        if ( submitter == null )
                        {
                            submitter = new BLAST_LIST_SUBMIT( status, e, top_n );
                            submitter.start();
                        }
                        else
                            e.stream.println( "another list is processed right now" );
                    }
                }
                else
                {
                    Thread.sleep( 250 );
                    if ( submitter != null )
                    {
                        if ( !submitter.is_running() )
                            submitter = null;
                    }
                }
            }
            catch ( InterruptedException e )
            {
            }
        }

        if ( submitter != null )
        {
            submitter.done();
            try
            {
                submitter.join();
            }
            catch ( InterruptedException e )
            {
            }
        }
        return res;
    }

    public static String main_spark( final String ini_file )
    {
        String res = "";
        final String appName = BLAST_MAIN.class.getSimpleName();
        BLAST_SETTINGS settings = BLAST_SETTINGS_READER.read_from_json( ini_file, appName );

        System.out.println( String.format( "settings read from '%s'", ini_file ) );

        if ( !settings.valid() )
            System.out.println( settings.missing() );
        else
        {
            // print the settings before initializing everything
            System.out.println( settings.toString() );

            // create the global status to track REQUESTS and timing
            BLAST_STATUS status = new BLAST_STATUS( settings );

            // reader thread for console commands
            BLAST_CONSOLE cons = new BLAST_CONSOLE( status, 200 );
            cons.start();

            // reader-writer thread for communication-port
            BLAST_COMM comm = new BLAST_COMM( status, settings );
            comm.start();

            // reader for pubsub
            BLAST_PUBSUB pubsub = null;
            if ( settings.use_pubsub_source )
            {
                pubsub = new BLAST_PUBSUB( status, settings, 200 );
                pubsub.start();
            }

            // let the BLAST_SETTINGS_READER create the spark-configuration based on the settings
            SparkConf conf = BLAST_SETTINGS_READER.configure( settings );

            // create the spark-context and adjust some properties
            JavaSparkContext sc = new JavaSparkContext( conf );
            sc.setLogLevel( settings.spark_log_level );
            for ( String fn : settings.transfer_files )
                sc.addFile( fn );

            Broadcast< BLAST_LOG_SETTING > LOG_SETTING = sc.broadcast( settings.log );

            try
            {
                BLAST_DATABASE_MAP db_map = new BLAST_DATABASE_MAP( settings, LOG_SETTING, sc );

                BLAST_JOBS jobs = new BLAST_JOBS( settings, LOG_SETTING, sc, db_map, status );

                System.out.println( "spark-blast started..." );

                /* ************************************************ */
                res = main_spark_loop( status, jobs, settings.top_n, ini_file );
                /* ************************************************ */

                jobs.stop_all_jobs();
                comm.join();
                cons.join();

                if ( settings.use_pubsub_source && pubsub != null )
                    pubsub.join();

                System.out.println( "spark-blast done..." );
            }
            catch( Exception e )
            {
                System.out.println( String.format( "BLAST_MAIN : %s", e ) );
            }

            sc.close();
        }
        return res;
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
            System.out.println( "settings json-file missing" );
        else
        {
            String ini = args[ 0 ];
            while ( !ini.isEmpty() )
            {
                ini = main_spark( ini );
            }
        }
   }
}

