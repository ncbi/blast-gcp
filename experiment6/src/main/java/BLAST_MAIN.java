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

//import java.util.List;
//import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public final class BLAST_MAIN
{
    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
            System.out.println( "settings json-file missing" );
        else
        {
            final String appName = BLAST_MAIN.class.getSimpleName();
            String ini_path = args[ 0 ];
            BLAST_SETTINGS settings = BLAST_SETTINGS_READER.read_from_json( ini_path, appName );
            System.out.println( String.format( "settings read from '%s'", ini_path ) );
            if ( !settings.valid() )
                System.out.println( settings.missing() );
            else
            {
                System.out.println( settings.toString() );
                SparkConf conf = BLAST_SETTINGS_READER.configure( settings );

                JavaSparkContext sc = new JavaSparkContext( conf );
                sc.setLogLevel( settings.spark_log_level );
                for ( String fn : settings.transfer_files )
                    sc.addFile( fn );

                ConcurrentLinkedQueue< BLAST_REQUEST > requests = new ConcurrentLinkedQueue<>();
                AtomicBoolean running = new AtomicBoolean( false );
                BLAST_CONSOLE cons = new BLAST_CONSOLE( requests, running, 200, settings.top_n );
                cons.start();

                try
                {
                    BLAST_YARN_NODES nodes = new BLAST_YARN_NODES();

                    Broadcast< BLAST_SETTINGS > SETTINGS = sc.broadcast( settings );
                    BLAST_DATABASE db = new BLAST_DATABASE( settings, SETTINGS, sc, nodes );
                    System.out.println( "driver started..." );

                    BLAST_JOBS jobs = new BLAST_JOBS( settings, SETTINGS, sc, db, requests );

                    while( running.get() )
                    {
                        try
                        {
                            Thread.sleep( 500 );
                        }
                        catch ( InterruptedException e )
                        {
                        }
                    }

                    jobs.stop();
                    cons.join();

                    System.out.println( "driver done..." );
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "BLAST_MAIN : %s", e ) );
                }
            }
        }
   }
}

