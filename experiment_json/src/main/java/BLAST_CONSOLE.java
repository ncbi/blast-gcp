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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public final class BLAST_CONSOLE  extends Thread
{
    private final BLAST_STATUS status;

    private final Integer sleep_time;

    public BLAST_CONSOLE( final BLAST_STATUS a_status,
                          final Integer a_sleep_time )
    {
        this.status = a_status;
        this.sleep_time = a_sleep_time;
    }

    private void sleep_now()
    {
        try
        {
            Thread.sleep( sleep_time );
        }
        catch ( InterruptedException e )
        {
        }
    }

    private String get_line( BufferedReader br )
    {
        try
        {
            if ( br.ready() )
                return br.readLine().trim();
        }
        catch ( IOException e )
        {
        }
        return null;
    }

    @Override public void run()
    {
        BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );

        while( status.is_running() && br != null )
        {
            boolean do_sleep = true;

            String line = get_line( br );
            if ( line != null && !line.isEmpty() )
            {
                status.add_cmd( new CMD_Q_ENTRY( System.out, line ) );
                do_sleep = false;
            }

            if ( status.is_running() && do_sleep ) sleep_now();
        }
    }
}

