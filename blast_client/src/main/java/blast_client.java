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

class db_list extends for_each_line
{
    private final request_obj ro;

	public db_list( final blast_server_connection conn,
			        request_obj ro,
					final String db_locations )
	{
		super( conn, db_locations );
		this.ro = ro;			
	}

	@Override public void on_line( final String line )
	{
		String reply = conn.call_server( ro.toJson( line ) );

		String result_filename = String.format( "%s-%s.res", ro.RID, json_utils.get_last_part( line ) );
		int l = reply.length();
		if ( l > 0 )
		{
			json_utils.writeStringToFile( result_filename, reply );
			tb_list tbl = new tb_list( reply );
			tbl.write_to_file( String.format( "%s.asn1", result_filename ) );
		}
		System.out.println( String.format( "'%s' written ( l = %d )", result_filename, l ) );
	}
}

class request_list extends for_each_line
{
	private final String db_locations;

	public request_list( final blast_server_connection conn,
						 final String request_list_path,
						 final String db_locations )
	{
		super( conn, request_list_path );
		this.db_locations = db_locations;
	}

	@Override public void on_line( final String line )
	{
		String org_query = json_utils.readFileAsString( line );
		if ( !org_query.isEmpty() )
		{
			db_list list = new db_list( conn, new request_obj( org_query ), db_locations );
			list.run();
		}
		else
			System.out.println( String.format( "request: '%s' not found or empty", line ) );
	}
}

class runner extends Thread
{
	private final blast_server_connection conn;
	private final String request_list_path;
	private final String db_locations;

	public runner( final blast_server_connection conn,
  				   final String request_list_path,
				   final String db_locations )
	{
		this.conn = conn;
		this.request_list_path = request_list_path;
		this.db_locations = db_locations;
	}
	
	public void run()
	{
		request_list list = new request_list( conn, request_list_path, db_locations );
		list.run();
	}
}

public final class blast_client
{
    public static void main( String[] args )
    {
        if ( args.length < 3 )
            System.out.println( "port-number, request-list, and db_location are missing" );
        else
        {
			int port = Integer.parseInt( args[ 0 ].trim() );
			blast_server_connection conn = new blast_server_connection( "./blast_server", port );

			if ( conn != null )
			{
				String request_list_path = args[ 1 ];
				String db_locations = args[ 2 ];

				for ( int i = 0; i < 5; ++i )
					( new runner( conn, request_list_path, db_locations ) ).start();

				conn.close();
			}
        }
   }

}
