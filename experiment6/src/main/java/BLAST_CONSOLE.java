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

public final class BLAST_CONSOLE  extends Thread
{
    private final BLAST_STATUS status;
    private final BLAST_SETTINGS settings;

    private final Integer sleep_time;

    public BLAST_CONSOLE( final BLAST_STATUS a_status,
                          final BLAST_SETTINGS a_settings,
                          final Integer a_sleep_time )
    {
        this.status = a_status;
        this.settings = a_settings;
        this.sleep_time = a_sleep_time;
    }

    @Override public void run()
    {
        Console cons = System.console();

        while( status.is_running() && cons != null )
        {
            String line = cons.readLine().trim();
            if ( !line.isEmpty() )
            {
                if ( line.equals( "status" ) )
                    System.out.println( String.format( "%s\n", status) );
                else if ( line.equals( "exit" ) )
                    status.stop();
                else
                    status.add_cmd( line );
            }
            else if ( status.is_running() )
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

