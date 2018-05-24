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

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

class BLAST_JOBS
{
    private final List< BLAST_JOB > jobs;
    private final BLAST_SETTINGS settings;
    private final Broadcast< BLAST_LOG_SETTING > LOG_SETTING;
    private final JavaSparkContext sc;
    private final BLAST_DATABASE_MAP db;
    private final BLAST_STATUS status;

    public BLAST_JOBS( final BLAST_SETTINGS a_settings,
                       Broadcast< BLAST_LOG_SETTING > a_LOG_SETTING,
                       JavaSparkContext a_sc,
                       BLAST_DATABASE_MAP a_db,
                       BLAST_STATUS a_status )
    {
        jobs = new ArrayList<>();
        this.settings = a_settings;
        this.LOG_SETTING = a_LOG_SETTING;
        this.sc = a_sc;
        this.db = a_db;
        this.status = a_status;
        a_status.set_jobs( this );

        start_jobs();
    }

    private void start_jobs()
    {
        for ( int i = 0; i < settings.parallel_jobs; ++i )
            jobs.add( new BLAST_JOB( settings, LOG_SETTING, sc, db, status ) );
        for ( BLAST_JOB j : jobs )
            j.start();
        status.set_parallel_jobs( jobs.size() );
    }

    private void try_join( BLAST_JOB j )
    {
        try
        {
            j.join();
        }
        catch( InterruptedException e )
        {
        }
    }

    private void stop_jobs( List< BLAST_JOB > l )
    {
        for ( BLAST_JOB j : l )
            j.signal_done();
        for ( BLAST_JOB j : l )
            try_join( j );
    }

    public void stop_all_jobs()
    {
        stop_jobs( jobs );
    }

    public void restart_jobs()
    {
        stop_jobs( jobs );
        start_jobs();
    }
 
    public void update_ack( final REQUESTQ_ENTRY re )
    {
        for ( BLAST_JOB j : jobs )
            j.update_ack( re );
    }

    private void reduce_n( Integer N )
    {
        System.out.println( String.format( "removing %d jobs", N ) );
        List< BLAST_JOB > to_stop = new ArrayList<>();
        for ( int i = 0; i < N; ++i )
            to_stop.add( jobs.remove( 0 ) );
        stop_jobs( to_stop );
        status.set_parallel_jobs( jobs.size() );
        System.out.println( String.format( "%d jobs removed, now : %d", N, jobs.size() ) );
    }

    private void add_n( Integer N )
    {
        System.out.println( String.format( "adding %d jobs", N ) );        
        List< BLAST_JOB > to_add = new ArrayList<>();
        for ( int i = 0; i < N; ++i )
            to_add.add( new BLAST_JOB( settings, LOG_SETTING, sc, db, status ) );
        for ( BLAST_JOB j : to_add )
        {
            j.start();
            jobs.add( j );
        }
        status.set_parallel_jobs( jobs.size() );
        System.out.println( String.format( "%d jobs added, now : %d", N, jobs.size() ) );
    }

    public void set( final String cmd )
    {
        try
        {
            Integer N = Integer.parseInt( cmd );
            Integer N_diff = N - jobs.size();
            if ( N_diff < 0 )
            {
                if ( N > 0 )
                    reduce_n( -N_diff );
            }
            else if ( N_diff > 0 )
                add_n( N_diff );
        }
        catch( Exception e )
        {
        }
    }
  
}
