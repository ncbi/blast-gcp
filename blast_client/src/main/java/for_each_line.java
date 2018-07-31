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

public class for_each_line
{
	protected final blast_server_connection conn;
	private final String path;

	public for_each_line( final blast_server_connection conn, final String path )
	{
		this.conn = conn;
		this.path = path;
	}

	public void run()
	{
		try
		{
		    BufferedReader reader = new BufferedReader( new FileReader( path ) );
			String line;
			while ( ( line = reader.readLine() ) != null )
			{
				String trimmed = line.trim();
				if ( !trimmed.isEmpty() && !trimmed.startsWith( "#" ) )
					on_line( trimmed );
			}
		    reader.close();
		}
        catch( Exception e )
        {
            System.out.println( String.format( "for_each_line : %s", e ) );
        }
	}

	public void on_line( final String line )
	{
	}
}
