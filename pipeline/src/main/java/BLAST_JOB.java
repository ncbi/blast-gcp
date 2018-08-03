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
import org.apache.spark.SparkFiles;

class BLAST_JOB extends Thread
{
    private final BLAST_SETTINGS settings;
    private Broadcast< BLAST_LOG_SETTING > LOG_SETTING;
    private Broadcast< String > LIB_NAME; // name of the jni-so to use
    private final JavaSparkContext sc;
    private final BLAST_DATABASE_MAP db;
    private final AtomicBoolean running;
    private final BLAST_STATUS status;
    private REQUESTQ_ENTRY entry;

    public BLAST_JOB( final BLAST_SETTINGS settings,
                      Broadcast< BLAST_LOG_SETTING > a_LOG_SETTING,
                      Broadcast< String > a_LIB_NAME,
                      JavaSparkContext sc,
                      BLAST_DATABASE_MAP a_db,
                      BLAST_STATUS a_status )
    {
        this.settings = settings;
        this.LOG_SETTING = a_LOG_SETTING;
        this.LIB_NAME = a_LIB_NAME;
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

    private JavaRDD< BLAST_TB_LIST_LIST > perform_dummpy_prelim_search_and_traceback(
                    final JavaRDD< BLAST_DATABASE_PART > SRC,	// array of blast-db's to operate on
                    Broadcast< BLAST_LOG_SETTING > SE,			// log settings
                    Broadcast< String > LIB_N,					// name of the jni-so to use
                    Broadcast< BLAST_REQUEST > REQ,				// the object representing the request
                    LongAccumulator ERRORS )					// the accumulator
    {
        return SRC.flatMap( item ->
        {
            ArrayList< BLAST_TB_LIST_LIST > res = new ArrayList<>(); // the list of traceback-lists ( result )
            BLAST_LOG_SETTING log = SE.getValue();
            BLAST_DATABASE_PART part = item;

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
            return res.iterator();
        } ).cache();
	}

    private JavaRDD< BLAST_TB_LIST_LIST > perform_jni_prelim_search_and_traceback(
                    final JavaRDD< BLAST_DATABASE_PART > SRC,	// array of blast-db's to operate on
                    Broadcast< BLAST_LOG_SETTING > SE,			// log settings
                    Broadcast< String > LIB_N,					// name of the jni-so to use
                    Broadcast< BLAST_REQUEST > REQ,				// the object representing the request
                    LongAccumulator ERRORS )					// the accumulator
    {
        return SRC.flatMap( item ->
        {
            ArrayList< BLAST_TB_LIST_LIST > res = new ArrayList<>(); // the list of traceback-lists ( result )
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

            BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( LIB_N.getValue(), part, log );
            if ( blaster != null )
            {
                try
                {
                    if ( !part.present() )
                    {
                        if ( part.copy() == 0 )
                            BLAST_SEND.send( log, String.format( "copy-failed,%s", part ) );
                        else if ( !part.present() )
                            BLAST_SEND.send( log, String.format( "still-not-present,%s", part ) );
                    }

					if ( log.job_start )
						BLAST_SEND.send( log, String.format( "S 0 %s %s", part.volume.name, req.id ) );

                    BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req, log.jni_log_level );
                    if ( search_res != null )
                    {
                        try
                        {
							int n_tb_results = 0;
                            BLAST_TB_LIST [] tb_results = blaster.jni_traceback( search_res, part, req, log.jni_log_level );
                            if ( tb_results != null )
                            {
                                for ( BLAST_TB_LIST tbl : tb_results )
                                    res.add( new BLAST_TB_LIST_LIST( tbl ) );
								n_tb_results = tb_results.length;
                            }

							if ( log.job_done )
								BLAST_SEND.send( log, String.format( "D %d %s %s", n_tb_results, part.volume.name, req.id ) );
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

            return res.iterator();
        } ).cache();
    }


    private JavaRDD< tb_list > perform_server_prelim_search_and_traceback(
                    final JavaRDD< BLAST_DATABASE_PART > SRC,	// array of blast-db's to operate on
                    Broadcast< BLAST_LOG_SETTING > SE,			// log settings
                    Broadcast< String > LIB_N,					// name of the jni-so to use
                    Broadcast< BLAST_REQUEST > REQ,				// the object representing the request
                    LongAccumulator ERRORS )					// the accumulator
    {
        return SRC.flatMap( item ->
        {
            ArrayList< tb_list > res = new ArrayList<>(); // the list of traceback-lists ( result )
            BLAST_LOG_SETTING log = SE.getValue();
            BLAST_DATABASE_PART part = item;
            BLAST_REQUEST req = REQ.getValue();

            // see if we are at a different worker-id now
            if ( log.worker_shift )
            {
                String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                if ( !curr_worker_name.equals( part.worker_name ) )
                    BLAST_SEND.send( log,
                                     String.format( "pre worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
            }

			Boolean present = part.present();
            if ( !present )
            {
                if ( part.copy() == 0 )
                    BLAST_SEND.send( log, String.format( "copy-failed,%s", part ) );
                else
				{
					present = part.present();
					if ( !present )
                    	BLAST_SEND.send( log, String.format( "still-not-present,%s", part ) );
				}
            }

			if ( present )
			{
				int n_results = 0;
				if ( log.job_start )
					BLAST_SEND.send( log, String.format( "S 0 %s %s", part.volume.name, req.id ) );

				String server_executable = SparkFiles.get( "blast_server" );
				blast_server_connection conn = BLAST_SERVER_SINGLETON.get( server_executable, 12345 );
				if ( conn != null )
				{
					server_request_obj ro = new server_request_obj( req );
					String reply = conn.call_server( ro.toJson( part.db_spec ) );

					if ( reply.length() > 5 )
					{
						tb_list reply_list = new tb_list( reply, req.top_n_traceback );
						if ( reply_list != null )
						{
				        	res.add( reply_list );
							n_results = reply_list.count();
						}
					}
				}
				if ( log.job_done )
					BLAST_SEND.send( log, String.format( "D %d %s %s", n_results, part.volume.name, req.id ) );
			}

            return res.iterator();
        } ).cache();
    }

    /* ===========================================================================================
            group all Traceback-results into a 'Iterable' of them on one worker-node

            IN  :   SRC: JavaPairDStream < REQ.ID, BLAST_TB_LIST >
            OUT :   JavaPairDStream < NR REQ.ID, Iterable< BLAST_TB_LIST > >
       =========================================================================================== */
    private BLAST_TB_LIST_LIST reduce_results_BLAST_TB_LIST_LIST( final JavaRDD< BLAST_TB_LIST_LIST > SRC )
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

    private tb_list reduce_results_tb_list( final JavaRDD< tb_list > SRC )
    {
        return SRC.reduce( ( item1, item2 ) ->
        {
            if ( item1 == null )
                return item2;
            if ( item2 == null )
                return item1;
            return new tb_list( item1, item2 );
        } );
    }

	private void write_result_bytebuffer( final String req_id, final ByteBuffer buf )
	{
        if ( settings.gs_or_hdfs.contains( "hdfs" ) )
        {
            String fn = String.format( settings.hdfs_result_file, req_id );
            String path = String.format( "%s/%s", settings.hdfs_result_dir, fn );
            Integer uploaded = BLAST_HADOOP_UPLOADER.upload( path, buf );
        }
        if ( settings.gs_or_hdfs.contains( "gs" ) )
        {
            String gs_result_key = String.format( settings.gs_result_file, req_id );
            Integer uploaded = BLAST_GS_UPLOADER.upload( settings.gs_result_bucket, gs_result_key, buf );
        }
	}

    private int write_results_BLAST_TB_LIST_LIST( final String req_id, final BLAST_TB_LIST_LIST ll )
    {
        int sum = 0;

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

		write_result_bytebuffer( req_id, buf );
        return sum;
    }

    private int write_results_tb_list( final String req_id, final tb_list list )
    {
        int sum = 0;

        for ( int i = 0; i < list.count(); ++i )
            sum += list.results[ i ].length();

        byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
        byte[] seq_annot_suffix = { 0, 0, 0, 0, 0, 0, 0, 0 };
        sum = sum + seq_annot_prefix.length + seq_annot_suffix.length;
        ByteBuffer buf = ByteBuffer.allocate( sum );

        buf.put( seq_annot_prefix );
        for ( int i = 0; i < list.results.length; ++i )
            list.results[ i ].put_to_ByteBuffer( buf );
        buf.put( seq_annot_suffix );

		write_result_bytebuffer( req_id, buf );
		return sum;
	}

	private int handle_request_without_search_and_traceback( final BLAST_REQUEST request,
                                 							  BLAST_DATABASE blast_db,
                                 							  LongAccumulator ERRORS )
	{
		int res = 0;
        final Broadcast< BLAST_REQUEST > REQ = sc.broadcast( request );
        try
        {
            final JavaRDD< BLAST_TB_LIST_LIST > RESULTS = perform_dummpy_prelim_search_and_traceback(
										blast_db.DB_SECS, LOG_SETTING, LIB_NAME, REQ, ERRORS );
            BLAST_TB_LIST_LIST result = reduce_results_BLAST_TB_LIST_LIST( RESULTS );
            if ( result != null )
                res = write_results_BLAST_TB_LIST_LIST( request.id, result );
        }
        catch ( Exception e )
        {
        }
		return res;
	}

    private int handle_request_with_jni( final BLAST_REQUEST request,
                                 		  BLAST_DATABASE blast_db,
                                 		  LongAccumulator ERRORS )
    {
		int res = 0;
        final Broadcast< BLAST_REQUEST > REQ = sc.broadcast( request );
        try
        {
            final JavaRDD< BLAST_TB_LIST_LIST > RESULTS = perform_jni_prelim_search_and_traceback(
										blast_db.DB_SECS, LOG_SETTING, LIB_NAME, REQ, ERRORS );
            BLAST_TB_LIST_LIST result = reduce_results_BLAST_TB_LIST_LIST( RESULTS );
            if ( result != null )
                res = write_results_BLAST_TB_LIST_LIST( request.id, result );
        }
        catch ( Exception e )
        {
        }
		return res;
    }

    private int handle_request_with_server( final BLAST_REQUEST request,
                                 		  	 BLAST_DATABASE blast_db,
                                 		  	 LongAccumulator ERRORS )
    {
		int res = 0;
        final Broadcast< BLAST_REQUEST > REQ = sc.broadcast( request );
        try
        {
            final JavaRDD< tb_list > RESULTS = perform_server_prelim_search_and_traceback(
										blast_db.DB_SECS, LOG_SETTING, LIB_NAME, REQ, ERRORS );

			/* !!! there will be a different result-java-class, because of the list of integer-tie-brackers */
            tb_list result = reduce_results_tb_list( RESULTS );
            if ( result != null )
                res = write_results_tb_list( request.id, result );
        }
        catch ( Exception e )
        {
        }
		return res;
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
            entry = status.get_request();
            if ( entry != null )
            {
                BLAST_DATABASE blast_db = db.get( entry.request.db );
                if ( blast_db != null )
                {
					// write the running-status
                    String gs_status_key = String.format( settings.gs_status_file, entry.request.id );
                    BLAST_GS_UPLOADER.upload( settings.gs_status_bucket, gs_status_key, settings.gs_status_running );

					// make an error-accumulator for this job
                    LongAccumulator ERRORS = sc.sc().longAccumulator();

        			Long started_at = System.currentTimeMillis();
					status.inc_running_jobs( entry.request.id );

					int sum = 0;
					// let us have 3 different handlers:
					if ( status.get_skip_search_and_tb() )
					{
						sum = handle_request_without_search_and_traceback( entry.request, blast_db, ERRORS );
					}
					else
					{
						if ( status.get_use_jni() )
						{
                    		sum = handle_request_with_jni( entry.request, blast_db, ERRORS );
						}
						else
						{
                    		sum = handle_request_with_server( entry.request, blast_db, ERRORS );
						}
					}

					long elapsed = System.currentTimeMillis() - started_at;
				    if ( settings.log.log_final )
				        System.out.println( String.format( "[%s] %d bytes summed up (%,d ms)", entry.request.id, sum, elapsed ) );

        			status.dec_running_jobs( entry.request.id );

					// to let the status keep track of average time and requests handled
                    status.after_request( elapsed );

					// print the new average...
                    System.out.println( String.format( "avg time = %,d ms", status.get_avg() ) );

					// write status: done or error depending on the error-accumulator...
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
					// ack the pubsub-queue if we have a ack_id ( we do not have one if the request did not come from pubsub! )
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

