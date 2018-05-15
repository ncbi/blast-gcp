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

class BLAST_JOB extends Thread
{
    private final BLAST_SETTINGS settings;
    private Broadcast< BLAST_SETTINGS > SETTINGS;
    private final JavaSparkContext sc;
    private final BLAST_DATABASE_MAP db;
    private final ConcurrentLinkedQueue< BLAST_REQUEST > requestq;
    private final AtomicBoolean running;
    private final BLAST_STATUS status;

    public BLAST_JOB( final BLAST_SETTINGS settings,
                      Broadcast< BLAST_SETTINGS > SETTINGS,
                      JavaSparkContext sc,
                      BLAST_DATABASE_MAP a_db,
                      ConcurrentLinkedQueue< BLAST_REQUEST > a_requestq,
                      BLAST_STATUS a_status )
    {
        this.settings = settings;
        this.SETTINGS = SETTINGS;
        this.sc = sc;
        this.db = a_db;
        this.requestq = a_requestq;
        this.status = a_status;
        running = new AtomicBoolean( false );
    }

    public void signal_done()
    {
        running.set( false );
    }

    private JavaRDD< BLAST_TB_LIST_LIST > prelim_search_and_traceback(
                    final JavaRDD< BLAST_DATABASE_PART > SRC, Broadcast< BLAST_SETTINGS > SE, Broadcast< BLAST_REQUEST > REQ )
    {
        return SRC.flatMap( item ->
        {
            ArrayList< BLAST_TB_LIST_LIST > res = new ArrayList<>();
            BLAST_SETTINGS bls = SE.getValue();
            BLAST_DATABASE_PART part = item;
            BLAST_REQUEST req = REQ.getValue();

            // see if we are at a different worker-id now
            if ( bls.log.worker_shift )
            {
                String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                if ( !curr_worker_name.equals( part.worker_name ) )
                {
                    BLAST_SEND.send( bls.log,
                                     String.format( "pre worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
                }
            }

            BLAST_DB_SETTING db_setting = bls.db_list.get( req.db );
            BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( part, db_setting, bls.log );
            if ( blaster != null )
            {
                try
                {
                    BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req, bls.jni_log_level );
                    if ( search_res != null )
                    {
                        try
                        {
                            BLAST_TB_LIST [] tb_results = blaster.jni_traceback( search_res, part, req, bls.jni_log_level );
                            if ( tb_results != null )
                            {
                                for ( BLAST_TB_LIST tbl : tb_results )
                                    res.add( new BLAST_TB_LIST_LIST( tbl ) );
                            }
                        }
                        catch ( Exception e )
                        {
                            BLAST_SEND.send( bls.log,
                                             String.format( "traceback: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
                        }
                    }
                }
                catch ( Exception e )
                {
                    BLAST_SEND.send( bls.log,
                                     String.format( "prelim-search: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
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

    private long handle_request( final BLAST_REQUEST request, BLAST_DATABASE blast_db )
    {
        Long started_at = System.currentTimeMillis();
        long elapsed = 0L;

        final Broadcast< BLAST_REQUEST > REQ = sc.broadcast( request );

        final JavaRDD< BLAST_TB_LIST_LIST > RESULTS = prelim_search_and_traceback( blast_db.DB_SECS, SETTINGS, REQ );

        BLAST_TB_LIST_LIST the_result = null;
        try
        {
            the_result = reduce_results( RESULTS );
            elapsed = write_results( request.id, the_result, started_at );
        }
        catch ( Exception e )
        {
            elapsed = System.currentTimeMillis() - started_at;
            System.out.println( String.format( "[ %s ] empty (%,d ms)", request.id, elapsed ) );
        }
        return elapsed;
    }

    @Override public void run()
    {
        running.set( true );

        while( running.get() )
        {
            BLAST_REQUEST request = requestq.poll();
            if ( request != null )
            {
                BLAST_DATABASE blast_db = db.get( request.db );
                if ( blast_db != null )
                {
                    long elapsed = handle_request( request, blast_db );
                    status.after_request( elapsed );
                    System.out.println( String.format( "avg time = %,d ms", status.get_avg() ) );
                }
                else
                {
                    System.out.println( String.format( "[ %s ] unknown database '%s'", request.id, request.db ) );
                }
            }
            else
            {
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                }
            }
        }
    }
}

