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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.Socket;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;

class BLAST_COMM_CLIENT extends Thread
{
    private final ConcurrentLinkedQueue< BLAST_REQUEST > request_q;
    private final ConcurrentLinkedQueue< String > cmd_q;
    private final BLAST_STATUS status;
    private final AtomicBoolean running;
    private final BLAST_SETTINGS settings;
    private final Socket socket;

    public BLAST_COMM_CLIENT( ConcurrentLinkedQueue< BLAST_REQUEST > a_request_q,
                              ConcurrentLinkedQueue< String > a_cmd_q,
                              BLAST_STATUS a_status,
                              AtomicBoolean a_running,
                              BLAST_SETTINGS a_settings,
                              Socket a_socket )
    {
        this.request_q = a_request_q;
        this.cmd_q = a_cmd_q;
        this.status = a_status;
        this.running = a_running;
        this.settings = a_settings;
        this.socket = a_socket;
    }
    
    @Override public void run()
    {
        try
        {
            socket.setTcpNoDelay( true );
            PrintStream ps = new PrintStream( socket.getOutputStream() );
            BufferedReader in = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
            String input;

            ps.printf( "spark-blast$" );
            while( running.get() && ( ( input = in.readLine() ) != null ) ) 
            {
                String trimmed = input.trim();
                if ( trimmed.equals( "bye" ) )
                    break;
                else if ( trimmed.equals( "status" ) )
                    ps.printf( "%s, q=%d\n", status, request_q.size() );
                else if ( trimmed.equals( "backlog" ) )
                    ps.printf( "%d\n", request_q.size() );
                else if ( trimmed.startsWith( "R" ) )
                    request_q.offer( BLAST_REQUEST_READER.parse( trimmed.substring( 1 ), settings.top_n ) );
                else
                    cmd_q.offer( trimmed );
                ps.printf( "spark-blast$" );
            }
            socket.close();
        }
        catch ( Exception e )
        {
            System.out.println( String.format( "BLAST_COMM_CLIENT: %s", e ) );
        }

    }
}

