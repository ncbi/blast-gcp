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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.concurrent.atomic.AtomicBoolean;

class BLAST_LIST_SUBMIT extends Thread
{
    private final BLAST_STATUS status;
    private final CMD_Q_ENTRY cmd;
    private final int top_n;
    private final AtomicBoolean running;

    public BLAST_LIST_SUBMIT( BLAST_STATUS a_status,
                              CMD_Q_ENTRY a_cmd,
                              int a_top_n )
    {
        this.status = a_status;
        this.cmd = a_cmd;        
        this.top_n = a_top_n;
        this.running = new AtomicBoolean( true );
    }

    private  void sleep_now( Integer ms )
    {
        try
        {
            Thread.sleep( ms );
        }
        catch ( InterruptedException e )
        {
        }
    }

    public boolean is_running() { return running.get(); }
    public void done() { running.set( false ); }

    @Override public void run()
    {
        try
        {
            String list_filename = cmd.line.substring( 1 );
            cmd.stream.println( String.format( "starting list : %s", list_filename ) );
            FileInputStream fs = new FileInputStream( list_filename );
            BufferedReader br = new BufferedReader( new InputStreamReader( fs ) );
            String line;
            String src = "";

            while ( is_running() && ( ( line = br.readLine() ) != null ) )
            {
                if ( !line.isEmpty() && !line.startsWith( "#" ) )
                {
                    if ( line.startsWith( ":src=" ) )
                    {
                        src = line.trim().substring( 5 );
                    }
                    else
                    {
                        String req_file = String.format( "%s/%s", src, line.trim() );
                        boolean done = false;
                        while( !done )
                        {
                            done = status.add_request_file( req_file, cmd.stream, top_n );
                            if ( !done )
                                sleep_now( 500 );
                        }
                        cmd.stream.println( String.format( "REQUEST-FILE '%s' added", req_file ) );
                    }
                }
            }
            br.close();
            cmd.stream.println( String.format( "done with list : %s", list_filename ) );
        }
        catch( Exception e )
        {
            cmd.stream.println( String.format( "BLAST_LIST_SUBMIT : %s", e ) );
        }
        done();
    }    
}
