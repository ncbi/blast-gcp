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

/**
 * Infrastructure-Class to read from the console
 * - stores reference to application-context
 * @see        BC_CONTEXT
*/
public final class BC_CONSOLE extends Thread
{
    private final BC_CONTEXT context;
    private final long sleep_time;

/**
 * create instance of BC_CONSOLE
 * - store reference application-context
 *
 * @param a_context    application-context
 * @see        BC_CONTEXT
*/
    public BC_CONSOLE( final BC_CONTEXT a_context )
    {
        context = a_context;
        sleep_time = context.get_settings().console_sleep_time;
    }

/**
 * overwritten run method of Thread-BC_CONSOLE
 * - create a buffered-reader for the system-input ( aka stdin )
 * - loop until application closed
 * - if the reader is ready to produce a line, read the line,
 *   convert the line into a command, store the command in
 *   application-context
 *
 * @see        BC_CONTEXT
*/
    @Override public void run()
    {
        BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );

        while( context.is_running() && br != null )
        {

            String line = null;
            try
            {
                if ( br.ready() )
                    line = br.readLine().trim();
            }
            catch ( IOException e ) { }

            if ( line != null && !line.isEmpty() )
            {
                context.push_command( line );
            }
            else if ( context.is_running() )
            {
                try
                {
                    Thread.sleep( sleep_time );
                }
                catch ( InterruptedException e ) { }
            }
        }
    }
}

