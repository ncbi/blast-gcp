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

import java.io.Console;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BLAST_CONSOLE  extends Thread
{
    private final ConcurrentLinkedQueue< BLAST_REQUEST > requests;
    private final AtomicBoolean running;
    private final Integer sleep_time;
    private final Integer dflt_top_n;

    public BLAST_CONSOLE( final ConcurrentLinkedQueue< BLAST_REQUEST > a_requests,
                          final AtomicBoolean a_running,
                          final Integer a_sleep_time,
                          final Integer a_dflt_top_n )
    {
        this.requests = a_requests;
        this.running = a_running;
        this.sleep_time = a_sleep_time;
        this.dflt_top_n = a_dflt_top_n;
    }

    @Override public void run()
    {
        Console cons = System.console();

        running.set( true );
        while( running.get() && cons != null )
        {
            String line = cons.readLine().trim();
            if ( !line.isEmpty() )
            {
                if ( line.startsWith( "R" ) )
                {
                    BLAST_REQUEST req = BLAST_REQUEST_READER.parse( line.substring( 1 ), dflt_top_n );
                    requests.offer( req );
                }
                else
                {
                    if ( line.equals( "exit" ) )
                        running.set( false );
                }
            }
            else if ( running.get() )
            {
                try
                {
                    Thread.sleep( sleep_time );
                }
                catch ( InterruptedException e )
                {
                }
            }
        }
    }

}
