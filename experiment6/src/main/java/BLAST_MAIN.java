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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public final class BLAST_MAIN
{

	private static void handle_skip( final BLAST_STATUS status, final CMD_Q_ENTRY e )
	{
        String[] splited = e.line.split( "\\s+" );
        if ( splited[ 1 ].equals( "on" ) )
        {
            status.set_skip_jni( true );
            e.stream.println( "skip-jni: on" );
        }
        else if ( splited[ 1 ].equals( "off" ) )
        {
            status.set_skip_jni( false );
            e.stream.println( "skip-jni: off" );
        }
        else
        {
            e.stream.printf( "unknown: '%s'\n", e.line );
        }
	}

	private static void handle_log( final BLAST_LOG_WRITER log_writer, final CMD_Q_ENTRY e )
	{
        String[] splited = e.line.split( "\\s+" );
        if ( splited[ 1 ].equals( "on" ) )
        {
			log_writer.console_out( true );
            e.stream.println( "log: on" );
        }
        else if ( splited[ 1 ].equals( "off" ) )
        {
			log_writer.console_out( false );
            e.stream.println( "log: off" );
        }
        else if ( splited[ 1 ].equals( "file" ) )
        {
			log_writer.set_filename( splited[ 2 ] );
            e.stream.println( String.format( "log-file: '%s'", log_writer.get_filename() ) );
        }
        else if ( splited[ 1 ].equals( "status" ) )
        {
			if ( log_writer.console_status() )
            	e.stream.println( String.format( "log-console: on, log-file: '%s'", log_writer.get_filename() ) );
			else
            	e.stream.println( String.format( "log-console: off, log-file: '%s'", log_writer.get_filename() ) );
        }
        else
        {
            e.stream.printf( "unknown: '%s'\n", e.line );
        }
	}

    /* the handling of all user-interface ( console as well as socket-communication is centralized here */
    public static String main_spark_loop( final BLAST_STATUS status,
										  final BLAST_LOG_WRITER log_writer,
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
                        status.add_request_file( e.line.substring( 1 ), e.stream, top_n, "" );
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
                    else if ( e.line.startsWith( "skip" ) )
						handle_skip( status, e );
					else if ( e.line.startsWith( "log" ) )
						handle_log( log_writer, e );
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

    public static String main_spark( final String ini_file, final String script_file )
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

            // writer for the log-messages from the workers
			BLAST_LOG_WRITER log_writer = new BLAST_LOG_WRITER( status );
			log_writer.start();

            // reader for the log-messages from the workers
            BLAST_LOG_RECEIVER log_receiver = new BLAST_LOG_RECEIVER( status, settings.log.port, log_writer );
            log_receiver.start();

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
            Broadcast< String > LIB_NAME = sc.broadcast( settings.lib_name );

            try
            {
                BLAST_DATABASE_MAP db_map = new BLAST_DATABASE_MAP( settings, LOG_SETTING, sc );

                BLAST_JOBS jobs = new BLAST_JOBS( settings, LOG_SETTING, LIB_NAME, sc, db_map, status );

                System.out.println( "spark-blast started..." );

				SCRIPT_PLAYER player = new SCRIPT_PLAYER( script_file, status );
				player.start();

                /* ********************************************************************** */
                res = main_spark_loop( status, log_writer, jobs, settings.top_n, ini_file );
                /* ********************************************************************** */

                jobs.stop_all_jobs();
				log_receiver.join();
				log_writer.join();
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
			String script = "";
			if ( args.length > 1 )
				script = args[ 1 ];
            while ( !ini.isEmpty() )
            {
                ini = main_spark( ini, script );
            }
        }
   }
}

