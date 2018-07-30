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
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringWriter;
import java.net.Socket;

public final class blast_client
{

	public static String process( String query, int port )
	{
		String res = "";
        try
        {
		    Socket socket = new Socket( "localhost", port );
		    socket.setTcpNoDelay( true );
		 	PrintStream ps = new PrintStream( socket.getOutputStream() );
            BufferedReader br = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );

			System.out.println( "reading" );
			res = br.readLine();
			System.out.println( String.format( "rec: %s", res ) );

			ps.printf( query );
			socket.shutdownOutput();

			res = br.readLine();
			System.out.println( String.format( "reading\nrec: %s", res ) );

            socket.close();
		}
        catch( Exception e )
        {
            System.out.println( String.format( "process : %s", e ) );
        }
		return res;
	}

	public static int run_cmd( String cmd, int port )
	{
		int res = -1;
		try
		{
			Process p = new ProcessBuilder( cmd, String.format( "%d", port ) ).start();
			BufferedReader br = new BufferedReader( new InputStreamReader( p.getErrorStream() ) );
			String line;
			while ( ( line = br.readLine()) != null )
			{
	  			System.out.println( line );
			}
			p.waitFor();
			res = p.exitValue();
		}
        catch( Exception e )
        {
            System.out.println( String.format( "run_cmd : %s", e ) );
        }
		return res;
	}

	private static String readFileAsString( String filePath )
	{
		try
		{
		    StringBuffer fileData = new StringBuffer();
		    BufferedReader reader = new BufferedReader( new FileReader( filePath ) );
			char[] buf = new char[ 1024 ];
		    int numRead = 0;
		    while ( ( numRead = reader.read( buf ) ) != -1 )
			{
		        String readData = String.valueOf( buf, 0, numRead );
		        fileData.append( readData );
		    }
		    reader.close();
		    return fileData.toString();
		}
        catch( Exception e )
        {
            System.out.println( String.format( "readFileAsString : %s", e ) );
        }
		return "";
    }

    public static void main( String[] args )
    {
        if ( args.length < 1 )
            System.out.println( "port-number missing" );
        else
        {
            String s_port = args[ 0 ];
			int port = Integer.parseInt( s_port.trim() );

			int res = run_cmd( "./blast_server", port );
			System.out.println( String.format( "return-code: %d", res ) );
			if ( res == 0 )
			{
				String query = readFileAsString( "blast_json.test.json" );
				if ( !query.isEmpty() )
				{
					String db_location = "/home/raetzw/BLAST_DB/nr_50M.01/nr_50M.01";
					request_obj ro = new request_obj( query, db_location );
					String reply = process( ro.toJson(), port );
					System.out.println( String.format( "reply: %s", reply ) );
				}
			}
        }
   }

}
