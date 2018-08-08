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

import java.io.FileReader;
import java.io.BufferedReader;

class db_list_2 extends for_each_line
{
	private final String executable;
	private int port;
    private final request_obj ro;
	private int idx;

	public db_list_2( final String executable,
					  int port,
			          request_obj ro,
					  final String db_locations,
					  int idx )
	{
		super( db_locations );
		this.executable = executable;
		this.port = port;
		this.ro = ro;
		this.idx = idx;
	}

	@Override public void on_line( final String line )
	{
		blast_server_connection conn = BLAST_SERVER_SINGLETON.get( executable, port );
		if ( conn != null )
		{
			String reply = conn.call_server( ro.toJson( line ) );

			String result_filename = String.format( "%s-%s.res", ro.RID, json_utils.get_last_part( line ) );
			int l = reply.length();
			if ( l > 0 )
			{
				json_utils.writeStringToFile( result_filename, reply );
				tb_list tbl = new tb_list( reply, 100 );
				tbl.write_to_file( String.format( "%s.asn1", result_filename ) );
			}
			System.out.println( String.format( "[%d] '%s' written ( l = %d )", idx, result_filename, l ) );
		}
		else
			System.out.println( String.format( "BLAST_SERVER_SINGLETON.get( %s, %d ) failed", executable, port ) );
	}
}

class request_list_2 extends for_each_line
{
	private final String executable;
	private int port;
	private final String db_locations;
	private int idx;

	public request_list_2( final String executable,
						   int port,
						   final String request_list_path,
						   final String db_locations,
						   int idx )
	{
		super( request_list_path );
		this.executable = executable;
		this.port = port;
		this.db_locations = db_locations;
		this.idx = idx;
	}

	@Override public void on_line( final String line )
	{
		String org_query = json_utils.readFileAsString( line );
		if ( !org_query.isEmpty() )
		{
			db_list_2 list = new db_list_2( executable, port, new request_obj( org_query ), db_locations, idx );
			list.run();
		}
		else
			System.out.println( String.format( "request: '%s' not found or empty", line ) );
	}
}

class runner_2 extends Thread
{
	private final String executable;
	private int port;
	private final String request_list_path;
	private final String db_locations;
	private int idx;

	public runner_2( final String executable,
				     int port,
  				     final String request_list_path,
				     final String db_locations,
				     int idx )
	{
		this.executable = executable;
		this.port = port;
		this.request_list_path = request_list_path;
		this.db_locations = db_locations;
		this.idx = idx;
	}
	
	public void run()
	{
		request_list_2 list = new request_list_2( executable, port, request_list_path, db_locations, idx );
		list.run();
	}
}

public class blast_runner_2
{
	public static void run( final String executable, int port, int num_threads,
					   	    final String request_list_path, final String db_locations )
	{
		for ( int i = 0; i < num_threads; ++i )
			( new runner_2( executable, port, request_list_path, db_locations, i ) ).start();
	}
}
