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

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Files;

class BLAST_SERVER_SINGLETON
{
    private static blast_server_connection conn = null;
    private static String lock_file_name = "/tmp/blast_server.lock";

	private static boolean file_exists( final String filename )
	{
		boolean res = false;
		try
		{
			File f = new File( filename );
			res = ( f.exists() && !f.isDirectory() );
		}
        catch( Exception e )
        {
        }
		return res;
	}

	private static boolean create_file( final String filename )
	{
		boolean res = false;
		try
		{
			File f = new File( filename );
			f.createNewFile();
			res = true;
		}
        catch( Exception e )
        {
        }
		return res;
	}

	private static boolean delete_file( final String filename )
	{
		boolean res = false;
		try
		{
			File f = new File( filename );
			res = f.delete();
		}
        catch( Exception e )
        {
        }
		return res;
	}

	/* We have to use 'synchronized' to protect this method from beeing executed in parallel by multiple threads.
	   But we have to also protect from beeing executed in parallel by multiple jvm's!
	   We can only do that by using a lock-file!
	*/
	synchronized private static blast_server_connection create_connection( final String executable, int port )
	{
        if ( conn == null )
            conn = new blast_server_connection( executable, port );
        return conn;
	}

    public static blast_server_connection get( final String executable, int port )
	{
		return create_connection( executable, port );
	}

}
