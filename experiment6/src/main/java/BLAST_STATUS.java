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

import java.io.PrintStream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import gov.nih.nlm.ncbi.blast.CustomLogger;

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
    private final AtomicBoolean skip_jni;
    private final ConcurrentHashMap< String, Integer > running_ids;
    private final ConcurrentLinkedQueue< REQUESTQ_ENTRY > request_q;
    private final ConcurrentLinkedQueue< String > ack_q;
    private final ConcurrentLinkedQueue< CMD_Q_ENTRY > cmd_q;
    private BLAST_JOBS jobs;

    private static final CustomLogger cl_js = new CustomLogger("projects/ncbi-sandbox-blast/logs/dataproc-job-stats", "blast-gcp", "global", null);

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
        skip_jni = new AtomicBoolean( false );
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
    public int get_running_jobs() { return running_jobs.get(); }
    public long get_total_requests() { return total_requests.get(); }
    public boolean get_skip_jni() { return skip_jni.get(); }
    public void set_skip_jni( boolean value ) { skip_jni.set( value ); }

    public int inc_running_jobs( final String id )
    {
        running_ids.put( id, 1 );
        int running_cnt = running_jobs.incrementAndGet();

        int currentTime = (int)(System.currentTimeMillis()/1000L);
        String logmsg = "rid=" + id
                + ";action=Starting Spark Run"
                + ";running_count=" + Integer.toString(running_cnt)
                + ";spark_start_time=" + Integer.toString(currentTime);
        cl_js.info(logmsg);

        return running_cnt;
    }

    public int dec_running_jobs( final String id )
    {
        running_ids.remove( id );
        int running_cnt = running_jobs.decrementAndGet();

        int currentTime = (int)(System.currentTimeMillis()/1000L);
        String logmsg = "rid=" + id
                + ";action=Endiing Spark Run"
                + ";running_count=" + Integer.toString(running_cnt)
                + ";spark_end_time=" + Integer.toString(currentTime);
        cl_js.info(logmsg);

        return running_cnt;
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

    public boolean add_request( REQUESTQ_ENTRY re, final PrintStream ps )
    {
        boolean res = !contains( re );
        if ( res )
        {
            int backlog_sz = inc_backlog();
            request_q.offer( re );
            if ( ps != null ) {
                ps.printf( "REQUEST '%s' added\n", re.request.id );
                int currentTime = (int)(System.currentTimeMillis()/1000L);
                String logmsg = "rid=" + re.request.id 
                        + ";action=Queuing to Backlog"
                        + ";backlog_size=" + Integer.toString(backlog_sz)
                        + ";backlog_queue_time=" + Integer.toString(currentTime);
                cl_js.info(logmsg);
            }
        }
        else
        {
            if ( ps != null )
                ps.printf( "REQUEST '%s' rejected\n", re.request.id );
        }
        return res;
    }

    public boolean add_request_string( final String req_string, final PrintStream ps, int top_n )
    {
        boolean res = false;
        if ( can_take() > 0 )
        {
            REQUESTQ_ENTRY re = BLAST_REQUEST_READER.parse_from_string( req_string, top_n, get_skip_jni() );
            if ( re != null )
                res = add_request( re, ps );
            else if ( ps != null )
                ps.printf( "invalid request '%s'\n", req_string );
        }
        return res;
    }

	// return 0 ... not enouth space in queue
    // return 1 ... added
	// return 2 ... filtered out
	// return 3 ... error adding
	// return 4 ... invalid request
    public int add_request_file( final String filename, final PrintStream ps, int top_n, final String program_filter )
    {
        int res = 0;
        if ( can_take() > 0 )
        {
            REQUESTQ_ENTRY re = BLAST_REQUEST_READER.parse_from_file( filename, top_n, get_skip_jni() );
            if ( re != null )
			{
				if ( program_filter.isEmpty() )
				{
                	if ( add_request( re, ps ) )
						res = 1;
					else
						res = 3;
				}
				else if ( re.request.db.startsWith( program_filter ) )
				{
                	if ( add_request( re, ps ) )
						res = 1;
					else
						res = 3;
				}
				else
				{
                	ps.printf( "'%s' filtered out by progam '%s'<>'%s'\n", filename, program_filter, re.request.db );
					res = 2;
				}
			}
            else if ( ps != null )
			{
                ps.printf( "invalid request in '%s'\n", filename );
				res = 4;
			}
        }
        return res;
    }

    public REQUESTQ_ENTRY get_request()
    {
        REQUESTQ_ENTRY res = request_q.poll();
        if ( res != null ) {
            int backlog_sz = dec_backlog();
            int currentTime = (int)(System.currentTimeMillis()/1000L);
            String logmsg = "rid=" + res.request.id 
                    + ";action=Queuing to Spark"
                    + ";backlog_size=" + Integer.toString(backlog_sz)
                    + ";spark_queue_time=" + Integer.toString(currentTime);
            cl_js.info(logmsg);
        }
        return res;
    }

    public void add_ack( final String ack_id ) { ack_q.offer( ack_id ); }
    public String get_ack() { return ack_q.poll(); }

    public void add_cmd( final CMD_Q_ENTRY entry ) { cmd_q.offer( entry ); }
    public CMD_Q_ENTRY get_cmd() { return cmd_q.poll(); }

    private int inc_backlog() { return backlog.incrementAndGet(); }
    public int dec_backlog() { return backlog.decrementAndGet(); }
    public int get_backlog() { return backlog.get(); }

    public int can_take() { return ( settings.max_backlog - get_backlog() ); }

    public void update_ack( final REQUESTQ_ENTRY re )
    {
        if ( is_a_running_id( re.request.id ) )
        {
            if ( jobs != null )
                jobs.update_ack( re );
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
        String S = String.format( "avg=%,d ms, parallel=%d, running=%d, backlog=%d n=%d",
            get_avg(), parallel_jobs.get(), running_jobs.get(), backlog.get(), total_requests.get() );
        S = S + String.format( "\nskip-jni: '%s'", Boolean.toString( get_skip_jni() ) );
        for ( String id : running_ids.keySet() )
            S = S + String.format( "\nrunning: %s", id );
        REQUESTQ_ENTRY[] waiting = request_q.toArray( new REQUESTQ_ENTRY[ request_q.size() ] );
        for ( REQUESTQ_ENTRY e : waiting )
            S = S + String.format( "\nwaiting: %s", e.request.id );

        return S;
    }
}

