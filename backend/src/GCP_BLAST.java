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

import java.util.*;
import java.io.*;

public final class GCP_BLAST
{
    private static void wait_for_console( final String exit_string, Integer sleeptime_ms )
    {
        Console cons = System.console();
        boolean running = true;
        
        while( running && cons != null )
        {
            running = ! cons.readLine().trim().equals( exit_string );
            if ( running )
            {
                try
                {
                    Thread.sleep( sleeptime_ms );
                }
                catch ( InterruptedException e )
                {
                }
            }
        }
    }
    
	public static void main( String[] args ) throws Exception
	{
        final String appName = GCP_BLAST.class.getSimpleName();
        final String master = args.length > 0 ? args[ 0 ] : "local[4]";

        List< String > files_to_transfer = new ArrayList<>();
        files_to_transfer.add( "blastjni.so" );
        
        GCP_BLAST_DRIVER driver = new GCP_BLAST_DRIVER( appName, master, files_to_transfer );
        driver.start();
        try
        {
            wait_for_console( "exit", 500 );
            driver.stop_blast();
            driver.join();
        }
        catch ( InterruptedException e )
        {
            System.out.println( String.format( "driver interrupted: %s", e ) );
        }
	}
}
