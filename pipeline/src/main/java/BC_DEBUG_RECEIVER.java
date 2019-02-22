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

import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

class BC_DEBUG_RECEIVER_CLIENT extends Thread
{
    private final BC_CONTEXT context;
    private final Socket socket;

    public BC_DEBUG_RECEIVER_CLIENT( BC_CONTEXT a_context, Socket a_socket )
    {
        context = a_context;
        socket = a_socket;
    }

    private void sleepFor( long milliseconds )
    {
        try { Thread.sleep( milliseconds ); }
        catch ( InterruptedException e ) { }
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
        try
        {
            socket.setTcpNoDelay( true );
            BufferedReader br = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
            String input;

            while( context.is_running() && br != null )
			{
		        boolean do_sleep = true;

		        String line = get_line( br );
		        if ( line != null && !line.isEmpty() )
		        {
					System.out.println( line.trim() );
		            do_sleep = false;
		        }

            	if ( context.is_running() && do_sleep )
					sleepFor( 100 );
            }
            socket.close();
        }
        catch ( Exception e )
        {
            System.out.println( String.format( "BC_DEBUG_RECEIVER_CLIENT: %s", e ) );
        }
    }
}


class BC_DEBUG_RECEIVER extends Thread
{
    private final BC_CONTEXT context;
	private List< BC_DEBUG_RECEIVER_CLIENT > clients;

    public BC_DEBUG_RECEIVER( BC_CONTEXT a_context )
    {
        context = a_context;
		clients = new ArrayList<>();
    }

    @Override public void run()
    {
        try
        {
			int port = context.get_settings().debug.port;
            System.out.println( String.format( "DEBUG_RECEIVER listening on port: %d", port ) );

            ServerSocket ss = new ServerSocket( port );
            while( context.is_running() )
            {
                try
                {
                    ss.setSoTimeout( 500 );
                    Socket client_socket = ss.accept();
                    BC_DEBUG_RECEIVER_CLIENT client = new BC_DEBUG_RECEIVER_CLIENT( context, client_socket );
					clients.add( client );
                    client.start();
                }
                catch ( Exception e )
                {
                    // do nothing if the loop times out...
                }
            }
            ss.close();
        }
        catch ( Exception e )
        {
            System.out.println( String.format( "BC_DEBUG_RECEIVER: %s", e ) );
        }
    }

	public void join_clients()
	{
		for( BC_DEBUG_RECEIVER_CLIENT client : clients )
		{
		    try { client.join(); }
		    catch( InterruptedException e ) { }
		}
		try { join(); }
		catch( InterruptedException e ) { }
	}
}

