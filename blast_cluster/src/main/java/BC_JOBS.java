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

package gov.nih.nlm.ncbi.blast_spark_cluster;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;

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

