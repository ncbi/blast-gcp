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
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.ByteBuffer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.LongAccumulator;


class BLAST_JOB extends Thread
{
    private final BLAST_SETTINGS settings;
    private Broadcast< BLAST_LOG_SETTING > LOG_SETTING;
    private final JavaSparkContext sc;
    private final BLAST_DATABASE_MAP db;
    private final AtomicBoolean running;
    private final BLAST_STATUS status;
    private REQUESTQ_ENTRY entry;

    public BLAST_JOB( final BLAST_SETTINGS settings,
                      Broadcast< BLAST_LOG_SETTING > a_LOG_SETTING,
                      JavaSparkContext sc,
                      BLAST_DATABASE_MAP a_db,
                      BLAST_STATUS a_status )
    {
        this.settings = settings;
        this.LOG_SETTING = a_LOG_SETTING;
        this.sc = sc;
        this.db = a_db;
        this.status = a_status;
        running = new AtomicBoolean( false );
        entry = null;
    }

    public void signal_done()
    {
        running.set( false );
    }

    public void update_ack( final REQUESTQ_ENTRY re )
    {
        if ( entry != null )
        {
            synchronized( entry )
            {
                if ( entry.ack_id != null && entry.request.id.equals( re.request.id ) )
                {
                    entry.ack_id = re.ack_id;
                }
            }
        }
    }

    private JavaRDD< BLAST_TB_LIST_LIST > prelim_search_and_traceback(
                    final JavaRDD< BLAST_DATABASE_PART > SRC,
                    Broadcast< BLAST_LOG_SETTING > SE,
                    Broadcast< BLAST_REQUEST > REQ,
                    LongAccumulator ERRORS )
    {
        return SRC.flatMap( item ->
        {
            ArrayList< BLAST_TB_LIST_LIST > res = new ArrayList<>();
            BLAST_LOG_SETTING log = SE.getValue();
            BLAST_DATABASE_PART part = item;
            BLAST_REQUEST req = REQ.getValue();

            // see if we are at a different worker-id now
            if ( log.worker_shift )
            {
                String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                if ( !curr_worker_name.equals( part.worker_name ) )
                {
                    BLAST_SEND.send( log,
                                     String.format( "pre worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
                }
            }

            if ( !req.skip_jni )
            {
                BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( part, log );
                if ( blaster != null )
                {
                    try
                    {
                        if ( !part.present() )
                        {
                            if ( part.copy() == 0 )
                                BLAST_SEND.send( log, String.format( "%s copy failed", part ) );
                            else if ( !part.present() )
                                BLAST_SEND.send( log, String.format( "%s still not present", part ) );
                        }

                        BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req, log.jni_log_level );
                        if ( search_res != null )
                        {
                            try
                            {
                                BLAST_TB_LIST [] tb_results = blaster.jni_traceback( search_res, part, req, log.jni_log_level );
                                if ( tb_results != null )
                                {
                                    for ( BLAST_TB_LIST tbl : tb_results )
                                        res.add( new BLAST_TB_LIST_LIST( tbl ) );
                                }
                            }
                            catch ( Exception e )
                            {
                                ERRORS.add( 1 );
                                BLAST_SEND.send( log,
                                                 String.format( "traceback: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
                            }
                        }
                    }
                    catch ( Exception e )
                    {
                        ERRORS.add( 1 );
                        BLAST_SEND.send( log,
                                         String.format( "prelim-search: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
                    }
                }
                else
                {
                    ERRORS.add( 1 );
                    BLAST_SEND.send( log,
                                     String.format( "prelim-search: no database found for selector: '%s'", req.db ) );
                }
            }

            return res.iterator();
        } ).cache();
    }

    /* ===========================================================================================
            group all Traceback-results into a 'Iterable' of them on one worker-node

            IN  :   SRC: JavaPairDStream < REQ.ID, BLAST_TB_LIST >
            OUT :   JavaPairDStream < NR REQ.ID, Iterable< BLAST_TB_LIST > >
       =========================================================================================== */
    private BLAST_TB_LIST_LIST reduce_results( final JavaRDD< BLAST_TB_LIST_LIST > SRC )
    {
        return SRC.reduce( ( item1, item2 ) ->
        {
            if ( item1 == null )
                return item2;
            if ( item2 == null )
                return item1;
            return new BLAST_TB_LIST_LIST( item1, item2 );
        } );
    }

    private long write_results( final String req_id, final BLAST_TB_LIST_LIST ll, Long started_at )
    {
        int sum = 0;
        long elapsed = 0L;

        for ( BLAST_TB_LIST e : ll.list )
            sum += e.asn1_blob.length;

        byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
        byte[] seq_annot_suffix = { 0, 0, 0, 0, 0, 0, 0, 0 };
        sum = sum + seq_annot_prefix.length + seq_annot_suffix.length;
        ByteBuffer buf = ByteBuffer.allocate( sum );

        buf.put( seq_annot_prefix );
        for ( BLAST_TB_LIST e : ll.list )
            buf.put( e.asn1_blob );
        buf.put( seq_annot_suffix );

        if ( settings.gs_or_hdfs.contains( "hdfs" ) )
        {
            String fn = String.format( settings.hdfs_result_file, req_id );
            String path = String.format( "%s/%s", settings.hdfs_result_dir, fn );
            Integer uploaded = BLAST_HADOOP_UPLOADER.upload( path, buf );

            elapsed = System.currentTimeMillis() - started_at;
            if ( settings.log.log_final )
                System.out.println( String.format( "[%s] %d bytes written to hdfs at '%s' (%,d ms)",
                        req_id, uploaded, path, elapsed ) );
        }
        if ( settings.gs_or_hdfs.contains( "gs" ) )
        {
            String gs_result_key = String.format( settings.gs_result_file, req_id );
            Integer uploaded = BLAST_GS_UPLOADER.upload( settings.gs_result_bucket, gs_result_key, buf );
            elapsed = System.currentTimeMillis() - started_at;

            if ( settings.log.log_final )
                System.out.println( String.format( "[%s] %d bytes written to gs '%s':'%s' (%,d ms)",
                        req_id, uploaded, settings.gs_result_bucket, gs_result_key, elapsed ) );
        }

        if ( settings.gs_or_hdfs.contains( "time" ) )
        {
            elapsed = System.currentTimeMillis() - started_at;
            if ( settings.log.log_final )
                System.out.println( String.format( "[%s] %d bytes summed up (%,d ms) n=%,d",
                        req_id, sum, elapsed, ll.list.size() ) );
        }
        return elapsed;
    }

    private long handle_request( final BLAST_REQUEST request,
                                 BLAST_DATABASE blast_db,
                                 LongAccumulator ERRORS )
    {
        Long started_at = System.currentTimeMillis();
        long elapsed = 0L;

        status.inc_running_jobs( request.id );

        final Broadcast< BLAST_REQUEST > REQ = sc.broadcast( request );

        try
        {
            final JavaRDD< BLAST_TB_LIST_LIST > RESULTS = prelim_search_and_traceback( blast_db.DB_SECS, LOG_SETTING, REQ, ERRORS );
            BLAST_TB_LIST_LIST result = reduce_results( RESULTS );
            if ( result != null )
                elapsed = write_results( request.id, result, started_at );
        }
        catch ( Exception e )
        {
            elapsed = System.currentTimeMillis() - started_at;
            System.out.println( String.format( "[ %s ] empty (%,d ms)", request.id, elapsed ) );
        }
        status.dec_running_jobs( request.id );
        return elapsed;
    }

    private void do_sleep()
    {
        try
        {
            Thread.sleep( 10 );
        }
        catch ( InterruptedException e )
        {
        }
    }

    @Override public void run()
    {
        running.set( true );

        while( running.get() )
        {
            boolean have_data;
            
            entry = status.get_request();
            if ( entry != null )
            {
                BLAST_DATABASE blast_db = db.get( entry.request.db );
                if ( blast_db != null )
                {
                    String gs_status_key = String.format( settings.gs_status_file, entry.request.id );
                    BLAST_GS_UPLOADER.upload( settings.gs_status_bucket, gs_status_key, settings.gs_status_running );

                    LongAccumulator ERRORS = sc.sc().longAccumulator();
                    long elapsed = handle_request( entry.request, blast_db, ERRORS );
                    status.after_request( elapsed );
                    System.out.println( String.format( "avg time = %,d ms", status.get_avg() ) );

                    gs_status_key = String.format( settings.gs_status_file, entry.request.id );
                    if ( ERRORS.value() == 0 )
                        BLAST_GS_UPLOADER.upload( settings.gs_status_bucket, gs_status_key, settings.gs_status_done );
                    else
                        BLAST_GS_UPLOADER.upload( settings.gs_status_bucket, gs_status_key, settings.gs_status_error );
                }
                else
                {
                    System.out.println( String.format( "[ %s ] unknown database '%s'", entry.request.id, entry.request.db ) );

                }
                synchronized( entry )
                {
                    if ( entry.ack_id != null )
                        status.add_ack( entry.ack_id );
                    entry = null;
                }
            }
            else
                do_sleep();
        }
    }
}

