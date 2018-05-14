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

class BLAST_STATUS
{
    private final AtomicLong total_requests;
    private final AtomicLong total_time;
    private final AtomicLong avg_time;
    private final AtomicInteger parallel_jobs;

    public BLAST_STATUS()
    {
        total_requests = new AtomicLong( 0L );
        total_time = new AtomicLong( 0L );
        avg_time = new AtomicLong( 0L );
        parallel_jobs = new AtomicInteger( 0 );
    }

    public void after_request( long ms )
    {
        long n = total_requests.incrementAndGet();
        long sum = total_time.addAndGet( ms );
        if ( n > 0 )
            avg_time.set( sum / n );
    }

    public long get_avg()
    {
        return avg_time.get();
    }

    public void set_parallel_jobs( int n )
    {
        parallel_jobs.set( n );
    }

    @Override public String toString()
    {
        return String.format( "avg=%,d ms, parallel jobs = %d", get_avg(), parallel_jobs.get() );
    }
}

