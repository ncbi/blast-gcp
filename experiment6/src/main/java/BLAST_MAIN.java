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

    public static void add_request( final REQUESTQ_ENTRY re,
                                    final BLAST_STATUS status )
    {
        if ( status.can_take() > 0 )
        {
            if ( re == null )
                System.out.println( "REQUEST invalid" );
            else
            {
                if ( !status.add_request( re ) )
                    System.out.println( String.format( "REQUEST '%s' rejected", re.request.id ) );
                else
                    System.out.println( String.format( "REQUEST '%s' added", re.request.id ) );
            }
        }
    }

    public static void main_spark_loop( final BLAST_STATUS status,
                                        final BLAST_JOBS jobs,
                                        int top_n )
    {
        String cmd;
        while( status.is_running() )
        {
            try
            {
                if ( ( cmd = status.get_cmd() ) != null )
                {
                    if ( cmd.startsWith( "J" ) )
                        jobs.set( cmd.substring( 1 ) );
                    else if ( cmd.equals( "exit" ) )
                        status.stop();
                    else if ( cmd.startsWith( "R" ) )
                        add_request( BLAST_REQUEST_READER.parse_from_string( cmd.substring( 1 ), top_n ), status );
                    else if ( cmd.startsWith( "F" ) )
                        add_request( BLAST_REQUEST_READER.parse_from_file( cmd.substring( 1 ), top_n ), status );
                }
                else
                    Thread.sleep( 250 );
            }
            catch ( InterruptedException e )
            {
            }
        }
    }

    public static void main_spark( final String ini_path )
    {
        final String appName = BLAST_MAIN.class.getSimpleName();
        BLAST_SETTINGS settings = BLAST_SETTINGS_READER.read_from_json( ini_path, appName );

        System.out.println( String.format( "settings read from '%s'", ini_path ) );

        if ( !settings.valid() )
            System.out.println( settings.missing() );
        else
        {
            // print the settings before initializing everything
            System.out.println( settings.toString() );

            // create the global status to track REQUESTS and timing
            BLAST_STATUS status = new BLAST_STATUS( settings );

            // reader thread for console commands
            BLAST_CONSOLE cons = new BLAST_CONSOLE( status, settings, 200 );
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

            Broadcast< BLAST_SETTINGS > SETTINGS = sc.broadcast( settings );

            try
            {
                BLAST_DATABASE_MAP db_map = new BLAST_DATABASE_MAP( settings, SETTINGS, sc );

                BLAST_JOBS jobs = new BLAST_JOBS( settings, SETTINGS, sc, db_map, status );

                System.out.println( "spark-blast started..." );

                /* ****************************************** */
                main_spark_loop( status, jobs, settings.top_n );
                /* ****************************************** */

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
        }
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
            System.out.println( "settings json-file missing" );
        else
        {
            //System.out.println( CONF_READER.read_conf( "dbs.json" ) );
            main_spark( args[ 0 ] );
        }
   }
}

