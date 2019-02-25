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

import java.nio.ByteBuffer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.SparkFiles;

class BC_JOB extends Thread
{
    private final BC_CONTEXT context;
    private final JavaSparkContext jsc;
    private Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS;
	private final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict;
	private final int id;

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
    }

    private void sleepFor( long milliseconds )
    {
        try { Thread.sleep( milliseconds ); }
        catch ( InterruptedException e ) { }
    }

	/* Attention: the Broadcast-Variable DBG has to be in the parameter-list, even
       if it is available as a field of the BC_JOB-class! If instead the class-field
       is referenced from within the closure, SPARK tries to serialize
       the whole BC_JOB-instance. This will fail: BC_JOB is not serializable.
       It will compile, but it will fail at runtime. */
	private void handle_request( Broadcast< BC_DEBUG_SETTINGS > DBG, BC_REQUEST request )
	{
		System.out.println( String.format( "JOB[%d] handles REQUEST[%s]", id, request.id ) );

		JavaRDD< BC_DATABASE_RDD_ENTRY > chunks = db_dict.get( request.db );
		if ( chunks == null )
			chunks = db_dict.get( request.db.substring( 0, 2 ) );

		if ( chunks != null )
		{
			final Broadcast< BC_REQUEST > REQUEST = jsc.broadcast( request );
			List< String > lines = new ArrayList<>();
			lines.add( String.format( "starting request '%s' at '%s'", request.id, BC_UTILS.datetime() ) );

			JavaRDD< String > RESULTS = chunks.flatMap( item ->
			{
				BC_DEBUG_SETTINGS debug = DBG.getValue();

				List< String > lst = new ArrayList<>();
				if ( !item.present() )
					lst.addAll( item.download() );
				BC_REQUEST req = REQUEST.getValue();

            	BLAST_LIB lib = new BLAST_LIB( "libblastjni.so" );
            	if ( lib != null )
            	{
					long starttime = System.currentTimeMillis();
					BLAST_HSP_LIST[] hsps = lib.jni_prelim_search( item, req, debug.jni_log_level );
					long finishtime = System.currentTimeMillis();

					if ( hsps == null )
						lst.add( String.format( "%s: %s x %s - search: returned null", item.workername(), item.name, req.id ) );
					else
					{
						lst.add( String.format( "%s: %s x %s - search: %d items ( %d ms )",
											    item.workername(), item.name, req.id, hsps.length, ( finishtime - starttime ) ) );
						
						starttime = System.currentTimeMillis();
						BLAST_TB_LIST [] tbs = lib.jni_traceback( hsps, item, req, debug.jni_log_level );
						finishtime = System.currentTimeMillis();

						if ( tbs == null )
							lst.add( String.format( "%s: %s x %s - traceback: returned null", item.workername(), item.name, req.id ) );
						else
						{
							lst.add( String.format( "%s: %s x %s - traceback: %d items ( %d ms )",
												    item.workername(), item.name, req.id, tbs.length, ( finishtime - starttime ) ) );
						}
					}
				}
				else
					lst.add( String.format( "%s: %s x %s - lib not initialized", item.workername(), item.name, req.id ) );

				return lst.iterator();
			});
			lines.addAll( RESULTS.collect() );

			lines.add( String.format( "request '%s' done at '%s'", request.id, BC_UTILS.datetime() ) );
			BC_UTILS.save_to_file( lines, String.format( "%s/REQ_%s.txt", context.settings.report_dir, request.id ) );

			System.out.println( String.format( "JOB[%d] REQUEST[%s] done", id, request.id ) );
		}
		else
			System.out.println( String.format( "JOB[%d] REQUEST[%s] : db '%s' not found", id, request.id, request.db ) );
	}

    @Override public void run()
	{
        while( context.is_running() )
        {
            BC_REQUEST request = context.get_request();
            if ( request != null )
				handle_request( DEBUG_SETTINGS, request );
			else
				sleepFor( context.settings.job_sleep_time );
		}
	}
}

public class BC_JOBS
{
    private final BC_CONTEXT context;
    private final JavaSparkContext jsc;
    private Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS;
	private final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict;
    private final List< BC_JOB > jobs;

    public BC_JOBS( final BC_CONTEXT a_context,
                    final JavaSparkContext a_jsc,
                    Broadcast< BC_DEBUG_SETTINGS > a_DEBUG_SETTINGS,
					final Map< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > a_db_dict )
    {
		context = a_context;
		jsc = a_jsc;
		DEBUG_SETTINGS = a_DEBUG_SETTINGS;
		db_dict = a_db_dict;

        jobs = new ArrayList<>();
        for ( int i = 0; i < context.settings.parallel_jobs; ++i )
		{
			BC_JOB job = new BC_JOB( context, jsc, DEBUG_SETTINGS, db_dict, i );
            jobs.add( job );
			job.start();
		}
    }

    public void join()
    {
        for ( BC_JOB job : jobs )
		{
		    try { job.join(); }
		    catch( InterruptedException e ) { }
		}
    }
}

