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
import java.io.FileReader;
import java.io.BufferedReader;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public final class LOCATION_REPORT
{
    private static int getReports( final String path, List< String > reports )
    {
        int res = 0;
        File[] files = new File( path ).listFiles();
        for ( File f : files )
        {
            if ( f.isFile() )
            {
                String fn = f.getName();
                if ( fn.endsWith( ".txt" ) )
                {
                    reports.add( String.format( "%s/%s", path, fn ) );
                    res++;
                }
            }
        }
        return res;
    }

    private static void report( final String caption, final Map< String, Set< String > > dict )
    {
        System.out.println( caption );
        for ( String key : dict.keySet() )
            System.out.println( String.format( "%s : %s", key, String.join( ",", dict.get( key ) ) ) );
    }

    private static void report_spread1( final String caption, final Map< Integer, Integer > dict )
    {
        System.out.println( caption );
        for ( Integer numWorkers : dict.keySet() )
        {
            Integer numRequests = dict.get( numWorkers );
            if ( numWorkers == 1 )
                System.out.println( String.format( "%d requests serviced by one worker", numRequests ) );
            else
                System.out.println( String.format( "%d requests serviced by %d workers", numRequests, numWorkers ) );
        }
    }

    private static void report_spread2( final String caption, final Map< Integer, Integer > dict )
    {
        System.out.println( caption );
        for ( Integer numDbs : dict.keySet() )
        {
            Integer numWorkers = dict.get( numDbs );
            if ( numWorkers == 1 )
                System.out.println( String.format( "one worker stores %d databases", numDbs ) );
            else
                System.out.println( String.format( "%d workers store %d databases", numWorkers, numDbs ) );
        }
    }

    private static void insert( Map< String, Set< String > > dict,
                                final String key,
                                final String value )
    {
        if ( dict.containsKey( key ) )
            dict.get( key ).add( value );
        else
        {
            Set< String > set = new HashSet<>();
            set.add( value );
            dict.put( key, set );
        }
    }

    private static void insert_spread( Map< Integer, Integer > dict,
                                       Integer key )
    {
        Integer value = 1;
        if ( dict.containsKey( key ) )
            value = dict.get( key ) + 1;
        dict.put( key, value );
    }

    private static void readLine( final String line,
                                  Map< String, Set< String > > by_worker,
                                  Map< String, Set< String > > by_db,
                                  Set< String > workers )
    {
        if ( line.startsWith( "starting" ) ) return;
        if ( line.startsWith( "request" ) ) return;
        String[] byColon = line.split( ":" );
        if ( byColon.length > 1 )
        {
            String[] bySlash = byColon[ 0 ].split( "/" );
            String[] byDash  = byColon[ 1 ].split( "-" );

            if ( bySlash.length > 0 && byDash.length > 0 )
            {
                String worker = bySlash[ 0 ].trim();
                String db     = byDash[ 0 ].trim();

                insert( by_worker, worker, db );
                insert( by_db,     db, worker );
                workers.add( worker );
            }
        }
    }

    private static void readReports( final List< String > reports,
                                     Map< String, Set< String > > by_worker,
                                     Map< String, Set< String > > by_db,
                                     Map< Integer, Integer > spread1,
                                     Map< Integer, Integer > spread2 )
    {
        for ( String fn : reports )
        {
            try
            {
                FileReader fr = new FileReader( fn );
                BufferedReader br = new BufferedReader( fr );
                String line = null;
                Set< String > workers = new HashSet<>();
                while ( ( line = br.readLine() ) != null )
                    readLine( line.trim(), by_worker, by_db, workers );
                insert_spread( spread1, workers.size() );
            }
            catch ( Exception e )
            {
                System.out.println( e );
            }
        }
        for ( String worker : by_worker.keySet() )
        {
            Integer numDbs = by_worker.get( worker ).size();
            insert_spread( spread2, numDbs );
        }
    }

    public static void main( String[] args ) throws Exception
    {
        String path = null;
        boolean verbose = false;

        for ( String arg : args )
        {
            if ( arg.equals( "--verbose" ) )
                verbose = true;
            else
                path = arg;
        }
        if ( path == null ) path = "report";

        List< String > reports = new ArrayList<>();
        if ( getReports( path, reports ) > 0 )
        {
            // key   : worker-name
            // value : list of db-chunks
            Map< String, Set< String > > by_worker = new HashMap<>();

            // key   : db-chunks
            // value : list of worker-names
            Map< String, Set< String > > by_db = new HashMap<>();

            // key   : number of workers used in a request
            // value : how many requests were service by that number of workers
            Map< Integer, Integer > spread1 = new HashMap<>();

            // key   : number of databases on a worker
            // value : how many workers have that number of databases
            Map< Integer, Integer > spread2 = new HashMap<>();

            readReports( reports, by_worker, by_db, spread1, spread2 );

            if ( verbose )
            {
                report( "\nDB chunks BY WORKERS:", by_worker );
                report( "\nWORKERS BY DB chunks:", by_db );
            }
            report_spread1( "\nSPREAD OF REQUESTS OVER WORKERS:", spread1 );
            report_spread2( "\nSPREAD OF DB chunks OVER WORKERS:", spread2 );
            System.out.println("Total number of DB chunks: {}".format(by_db.keySet().size()));
        }
    }
}

