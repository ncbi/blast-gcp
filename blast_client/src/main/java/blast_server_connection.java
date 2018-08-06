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

package gov.nih.nlm.ncbi.blast_client;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.StringWriter;
import java.net.Socket;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

class blast_server_connection
{
	private int port;
	private boolean available;

    public blast_server_connection( final String executable, int port )
    {
		this.port = port;
		if ( probe_server( port ) )
		{
			available = true;
			// i do not have to start the executable here!
			System.out.println( "probe_server() ---> true" );
		}
		else
		{
			// cannot open socket, have to start executable
			System.out.println( "probe_server() ---> false" );
			int pid = start_server( executable );
            System.out.println( String.format( "start_server() ---> %d", pid ) );
			available = ( pid > 1 );
		}
	}

	private int extract_pid( final String s )
	{
		int res = -1;
		Pattern p = Pattern.compile( "^\\D+(\\d+).*" );
		Matcher m = p.matcher( s );
		if ( m.find() )
			res = Integer.parseInt( m.group( 1 ) );
		return res;
	}

	private int start_server( final String executable )
	{
		int res = -1;
		try
		{
			Process p = new ProcessBuilder( executable, String.format( "%d", port ) ).start();
			BufferedReader br = new BufferedReader( new InputStreamReader( p.getErrorStream() ) );
			String line;
			int pid = -1;
			while ( ( line = br.readLine()) != null )
			{
				if ( line.startsWith( "blast_server daemon started" ) )
					pid = extract_pid( line );
			}
			p.waitFor();
			int exit_value = p.exitValue();
			if ( exit_value == 0 )
				res = pid;
		}
        catch( Exception e )
        {
            System.out.println( String.format( "blast_server_connection.start_server : %s", e ) );
        }
		return res;
	}

	public boolean probe_server( int port )
	{
		boolean res = false;
	    try
	    {
			Socket socket = new Socket( "localhost", port );
			socket.setTcpNoDelay( true );
			res = true;
	        socket.close();
		}
	    catch( Exception e )
	    {
	    }
		return res;
	}

	public String call_server( final String query )
	{
		String res = "";
		if ( available )
		{
		    try
		    {
				// we cannot 'reuse' the socket because the way the server is written:
		        // each request is processed in a new forked process
		        // this process will die after completion
				// this means the socket will be closed after the reply is sent
				Socket socket = new Socket( "localhost", port );
				socket.setTcpNoDelay( true );
			 	PrintStream ps = new PrintStream( socket.getOutputStream() );
		        BufferedReader br = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );

				res = br.readLine();

				ps.printf( query );
				socket.shutdownOutput();

				res = br.readLine();
		        socket.close();
			}
		    catch( Exception e )
		    {
		        System.out.println( String.format( "blast_server_connection.call_server : %s", e ) );
		    }
		}
		return res;
	}

}
