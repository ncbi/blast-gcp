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
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.ByteBuffer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.SparkFiles;

/**
 * internal class to process one job at a time
 * - stores reference to application-context
 * - stores reference to JavaSparkContext
 * - stores reference to broadcasted debug-settings
 * - stores reference to database-dictionary
 * - stores id of job-thread
 *
 * @see        BC_CONTEXT
*/
class BC_JOB extends Thread
{
    private final BC_CONTEXT context;
    private final JavaSparkContext jsc;
    private Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS;
    private final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict;
    private final int id;
    private final AtomicBoolean active;

/**
 * create instance of BC_JOB
 *
 * @param a_context             application-context
 * @param a_jsc                 JavaSparkContext
 * @param a_DEBUG_SETTINGS      broadcasted debug-settings
 * @param a_db_dict             dictionary of blast-database-RDDs
 * @param a_id                  Id of the thread
 *
 * @see        BC_CONTEXT
 * @see        BC_DEBUG_SETTINGS
 * @see        BC_DATABASE_RDD_ENTRY
*/
    public BC_JOB( final BC_CONTEXT a_context,
                   final JavaSparkContext a_jsc,
                   Broadcast< BC_DEBUG_SETTINGS > a_DEBUG_SETTINGS,
                   final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > a_db_dict,
                   final int a_id )
    {
        context = a_context;
        DEBUG_SETTINGS = a_DEBUG_SETTINGS;
        jsc = a_jsc;
        db_dict = a_db_dict;
        id = a_id;
        active = new AtomicBoolean( false );
    }

/**
 * check if this job is active ( processes a request )
*/
    public boolean is_active()
    {
        return active.get();
    }

/**
 * perform the mapping-operation on the request: this is the important call via jni to Blast
 * - find the correct database, based on the request.db field
 * - broadcast the request to the cluster
 * - perform the map-operation against the chunks-RDD via closure:
 *       > download chunk if neccessary
 *       > find instance of BLAST_LIB ( singleton )
 *       > perform lib.jni_prelim_search()
 *       > perform lib.jni_traceback()
 *       > return result-tuples
 * - collect container of result-tuples on master
 * - write results to local filesystem
 * - write report to local filesystem
 *
 * @param DBG           broadcast-variable to be used for debug-interface
 * @param request       request to be 'blasted' against one database-chunk
 * @see        BC_DEBUG_SETTINGS
 * @see        BC_REQUEST
 * @see        BC_DATABASE_RDD_ENTRY
 * @see        BC_UTILS
 * @see        BLAST_TB_LIST
 * @see        BLAST_LIB
 * @see        BLAST_HSP_LIST
*/
    private void handle_request( Broadcast< BC_DEBUG_SETTINGS > DBG, BC_REQUEST request )
    {
        /* Attention: the Broadcast-Variable DBG has to be in the parameter-list, even
           if it is available as a field of the BC_JOB-class! If instead the class-field
           is referenced from within the closure, SPARK tries to serialize
           the whole BC_JOB-instance. This will fail: BC_JOB is not serializable.
           It will compile, but it will fail at runtime. */

        System.out.println( String.format( "JOB[%d] handles REQUEST[%s]", id, request.id ) );
        long job_starttime = System.currentTimeMillis();

        JavaRDD< BC_DATABASE_RDD_ENTRY > chunks = db_dict.get( request.db );
        if ( chunks == null )
            chunks = db_dict.get( request.db.substring( 0, 2 ) );

        if ( chunks != null )
        {
            final Broadcast< BC_REQUEST > REQUEST = jsc.broadcast( request );
            List< String > lines = new ArrayList<>();
            lines.add( String.format( "starting request '%s' at '%s'", request.id, BC_UTILS.datetime() ) );

            /* ***** perform the map-operation on the worker-nodes ***** */
            JavaRDD< Tuple2< List< String >, List< BLAST_TB_LIST > > > RESULTS = chunks.map( item ->
            {
                BC_DEBUG_SETTINGS debug = DBG.getValue();

                List< String > str_lst = new ArrayList<>();
                List< BLAST_TB_LIST > tp_lst = new ArrayList<>();
                if ( !item.present() )
                    str_lst.addAll( item.download() );

                while ( !item.present() )
                {
                    try
                    {
                        Thread.sleep( 100 );
                    }
                    catch ( InterruptedException e ) { }
                }

                if ( item.present() )
                {
                    BC_REQUEST req = REQUEST.getValue();

                    BLAST_LIB lib = new BLAST_LIB( "libblastjni.so", false );
                    if ( lib != null )
                    {
                        long starttime = System.currentTimeMillis();
                        BLAST_HSP_LIST[] hsps = lib.jni_prelim_search( item, req, debug.jni_log_level );
                        long finishtime = System.currentTimeMillis();

                        if ( hsps == null )
                            str_lst.add( String.format( "%s: %s - search: returned null", item.workername(), item.chunk.name ) );
                        else
                        {
                            str_lst.add( String.format( "%s: %s - search: %d items ( %d ms )",
                                                    item.workername(), item.chunk.name, hsps.length, ( finishtime - starttime ) ) );

                            starttime = System.currentTimeMillis();
                            BLAST_TB_LIST [] tbs = lib.jni_traceback( hsps, item, req, debug.jni_log_level );
                            finishtime = System.currentTimeMillis();

                            if ( tbs == null )
                                str_lst.add( String.format( "%s: %s - traceback: returned null", item.workername(), item.chunk.name ) );
                            else
                            {
                                str_lst.add( String.format( "%s: %s - traceback: %d items ( %d ms )",
                                                        item.workername(), item.chunk.name, tbs.length, ( finishtime - starttime ) ) );

                                for ( BLAST_TB_LIST tb : tbs )
                                    tp_lst.add( tb );
                            }
                        }
                    }
                    else
                        str_lst.add( String.format( "%s: %s - lib not initialized", item.workername(), item.chunk.name ) );
                }
                else
                    str_lst.add( String.format( "%s: %s - failed to download", item.workername(), item.chunk.name ) );

                return new Tuple2<>( str_lst, tp_lst );
            });

            List< Tuple2< List< String >, List< BLAST_TB_LIST > > > l_res = RESULTS.collect();
            BC_RESULTS results = new BC_RESULTS();

            /* collect and write the report... */
            for ( Tuple2< List< String >, List< BLAST_TB_LIST > > item : l_res )
            {
                lines.addAll( item._1 );
                results.add( item._2 );
            }
            long job_finishtime = System.currentTimeMillis();
            lines.add( String.format( "request '%s' done at '%s' ( %d ms )", request.id, BC_UTILS.datetime(),
                                        ( job_finishtime - job_starttime ) ) );
            BC_UTILS.save_to_file( lines, String.format( "%s/REQ_%s.txt", context.settings.report_dir, request.id ) );

            results.sort();
            results.cutoff( request.top_n_traceback );
            String asn1_file_name = String.format( "%s/REQ_%s.asn1", context.settings.report_dir, request.id );
            BC_UTILS.write_to_file( results.to_bytebuffer(), asn1_file_name );

            System.out.println( String.format( "JOB[%d] REQUEST[%s] done", id, request.id ) );
        }
        else
            System.out.println( String.format( "JOB[%d] REQUEST[%s] : db '%s' not found", id, request.id, request.db ) );
    }

/**
 * overwritten run method of Thread-BC_JOB
 * - loop until application closed
 * - pull request from application-context
 * - handle request if request is not null, or sleep otherwise
 *
 * @see        BC_CONTEXT
 * @see        BC_REQUEST
*/
    @Override public void run()
    {
        while( context.is_running() )
        {
            BC_REQUEST request = context.get_request();
            if ( request != null )
            {
                active.set( true );
                handle_request( DEBUG_SETTINGS, request );
                active.set( false );
            }
            else
            {
                try
                {
                    Thread.sleep( context.settings.job_sleep_time );
                }
                catch ( InterruptedException e ) { }
            }
        }
    }
}

/**
 * class to process multiple jobs in parallel
 * - has list of jobs
 *
 * @see        BC_JOB
*/
public class BC_JOBS
{
    private final List< BC_JOB > jobs;

/**
 * create instance of BC_JOBS
 * - create as much job-instances as requested in application-settings
 *
 * @param context               application-context
 * @param jsc                   JavaSparkContext
 * @param DEBUG_SETTINGS        broadcasted debug-settings
 * @param db_dict               dictionary of blast-database-RDDs
 *
 * @see        BC_CONTEXT
 * @see        BC_DEBUG_SETTINGS
 * @see        BC_DATABASE_RDD_ENTRY
*/
    public BC_JOBS( final BC_CONTEXT context,
                    final JavaSparkContext jsc,
                    Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS,
                    final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict )
    {
        jobs = new ArrayList<>();
        for ( int i = 0; i < context.settings.parallel_jobs; ++i )
        {
            BC_JOB job = new BC_JOB( context, jsc, DEBUG_SETTINGS, db_dict, i );
            jobs.add( job );
            job.start();
        }
        context.set_jobs( this );
    }

/**
 * counts how many jobs are active
 *
 * return number of active jobs
*/
    public int active()
    {
        int res = 0;
        for ( BC_JOB j : jobs )
        {
            if ( j.is_active() ) res += 1;
        }
        return res;
    }

/**
 * wait for all jobs to finish
 *
*/
    public void join()
    {
        for ( BC_JOB job : jobs )
        {
            try { job.join(); }
            catch( InterruptedException e ) { }
        }
    }
}

