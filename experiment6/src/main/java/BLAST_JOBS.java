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
    private final Broadcast< BLAST_SETTINGS > SETTINGS;
    private final JavaSparkContext sc;
    private final BLAST_DATABASE db;
    private final ConcurrentLinkedQueue< BLAST_REQUEST > requests;

    public BLAST_JOBS( final BLAST_SETTINGS a_settings,
                       Broadcast< BLAST_SETTINGS > a_SETTINGS,
                       JavaSparkContext a_sc,
                       BLAST_DATABASE a_db,
                       ConcurrentLinkedQueue< BLAST_REQUEST > a_requests )
    {
        jobs = new ArrayList<>();
        this.settings = a_settings;
        this.SETTINGS = a_SETTINGS;
        this.sc = a_sc;
        this.db = a_db;
        this.requests = a_requests;

        for ( int i = 0; i < settings.parallel_jobs; ++i )
            jobs.add( new BLAST_JOB( settings, SETTINGS, sc, db, requests ) );
        for ( BLAST_JOB j : jobs )
            j.start();
    }

    public void stop()
    {
        for ( BLAST_JOB j : jobs )
            j.signal_done();
        for ( BLAST_JOB j : jobs )
        {
            try
            {
                j.join();
            }
            catch( InterruptedException e )
            {
            }
        }
    }
}
