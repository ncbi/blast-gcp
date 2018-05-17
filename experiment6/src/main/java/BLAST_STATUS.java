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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

class BLAST_STATUS
{
    private final BLAST_SETTINGS settings;
    private final AtomicLong total_requests;
    private final AtomicLong total_time;
    private final AtomicLong avg_time;
    private final AtomicInteger parallel_jobs;
    private final AtomicInteger running_jobs;
    private final AtomicInteger backlog;
    private final AtomicBoolean running;
    private final ConcurrentHashMap< String, Integer > running_ids;
    private final ConcurrentLinkedQueue< REQUESTQ_ENTRY > request_q;
    private final ConcurrentLinkedQueue< String > ack_q;
    private final ConcurrentLinkedQueue< String > cmd_q;
    private BLAST_JOBS jobs;

    public BLAST_STATUS( final BLAST_SETTINGS a_settings )
    {
        settings = a_settings;
        total_requests = new AtomicLong( 0L );
        total_time = new AtomicLong( 0L );
        avg_time = new AtomicLong( 0L );
        parallel_jobs = new AtomicInteger( 0 );
        running_jobs = new AtomicInteger( 0 );
        backlog = new AtomicInteger( 0 );
        running = new AtomicBoolean( true );
        running_ids = new ConcurrentHashMap<>();

        request_q = new ConcurrentLinkedQueue<>();
        ack_q = new ConcurrentLinkedQueue<>();
        cmd_q = new ConcurrentLinkedQueue<>();
        jobs = null;
    }

    public void set_jobs( BLAST_JOBS a_jobs ) { jobs = a_jobs; }

    public boolean is_running() { return running.get(); }
    public void stop() { running.set( false ); }

    public void after_request( long ms )
    {
        long n = total_requests.incrementAndGet();
        long sum = total_time.addAndGet( ms );
        if ( n > 0 )
            avg_time.set( sum / n );
    }

    public long get_avg() { return avg_time.get(); }
    public void set_parallel_jobs( int n ) { parallel_jobs.set( n ); }

    public int inc_running_jobs( final String id )
    {
        running_ids.put( id, 1 );
        return running_jobs.incrementAndGet();
    }

    public int dec_running_jobs( final String id )
    {
        running_ids.remove( id );
        return running_jobs.decrementAndGet();
    }

    private boolean is_a_running_id( final String id )
    {
        return running_ids.keySet().contains( id );
    }

    public boolean contains( REQUESTQ_ENTRY re )
    {
        boolean res = request_q.contains( re );
        if ( !res ) res = is_a_running_id( re.request.id );
        return res;
    }

    public boolean add_request( REQUESTQ_ENTRY re )
    {
        boolean res = !contains( re );
        if ( res )
        {
            inc_backlog();
            request_q.offer( re );
        }
        return res;
    }

    public REQUESTQ_ENTRY get_request()
    {
        REQUESTQ_ENTRY res = request_q.poll();
        if ( res != null )
            dec_backlog();
        return res;
    }

    public void add_ack( final String ack_id ) { ack_q.offer( ack_id ); }
    public String get_ack() { return ack_q.poll(); }

    public void add_cmd( final String cmd ) { cmd_q.offer( cmd ); }
    public String get_cmd() { return cmd_q.poll(); }

    private int inc_backlog() { return backlog.incrementAndGet(); }
    public int dec_backlog() { return backlog.decrementAndGet(); }
    public int get_backlog() { return backlog.get(); }

    public int can_take() { return ( settings.max_backlog - get_backlog() ); }

    public void update_ack( final REQUESTQ_ENTRY re )
    {
        if ( is_a_running_id( re.request.id ) )
        {
            if ( jobs != null )
                update_ack( re );
        }
        else if ( request_q.contains( re ) )
        {
            REQUESTQ_ENTRY[] waiting = request_q.toArray( new REQUESTQ_ENTRY[ request_q.size() ] );
            for ( REQUESTQ_ENTRY e : waiting )
            {
                if ( e.equals( re ) )
                    e.ack_id = re.ack_id;
            }
        }
    }

    @Override public String toString()
    {
        String S = String.format( "avg=%,d ms, parallel=%d, running=%d, backlog=%d",
            get_avg(), parallel_jobs.get(), running_jobs.get(), backlog.get() );
        for ( String id : running_ids.keySet() )
            S = S + String.format( "\nrunning: %s", id );
        REQUESTQ_ENTRY[] waiting = request_q.toArray( new REQUESTQ_ENTRY[ request_q.size() ] );
        for ( REQUESTQ_ENTRY e : waiting )
            S = S + String.format( "\nwaiting: %s", e.request.id );

        return S;
    }
}

