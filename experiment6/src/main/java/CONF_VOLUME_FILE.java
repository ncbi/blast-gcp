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

import java.io.Serializable;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.FileWriter;
import java.io.File;

import java.util.Base64;

import java.nio.file.Paths;
import java.nio.file.Files;

import com.google.api.services.storage.Storage;

public class CONF_VOLUME_FILE implements Serializable
{
    private final String name;   	/* for example : 'nt_50M.00.nsq' */
    private final String md5;    	/* for example : 'DBEis3HM8wceV0Pno3TPlQ==' */
    private final String local;  	/* for example : '/tmp/blast/db/nt_50M.00/nt_50M.00.nsq' */
    private final String lockfn;  	/* for example : '/tmp/blast/db/nt_50M.00/nt_50M.00.nsq.lock' */

	public CONF_VOLUME_FILE( final String a_name,
							 final String a_md5,
							 final String a_local )
	{
		name = a_name;
		md5 = a_md5;
		local = a_local;
		lockfn = String.format( "%s.lock", local );
	}

	public CONF_VOLUME_FILE( final CONF_VOLUME_FILE other,
							 final String location,
							 final String volume_name )
	{
		name = other.name;
		md5 = other.md5;
		local = String.format( "%s/%s/%s", location, volume_name, other.name );
		lockfn = String.format( "%s.lock", local );
	}

    @Override public String toString()
    {
        return String.format( "\t\t\t[ name:'%s', md5:'%s', local:'%s'\n",
								name, md5, local );
    }

	private boolean locked_f( final File f )
	{
		boolean res = true;
		try
		{
			FileInputStream f_in = new FileInputStream( f );
			BufferedReader r = new BufferedReader( new InputStreamReader( f_in ) );
			String line = r.readLine();
			res = !( line.equals( "done" ) );
			r.close();
			f_in.close();
		}
	    catch( Exception e )
	    {
	    }
		return res;
	}

	private boolean locked()
	{
		boolean res = false;
		File f = new File( lockfn );
		if ( f.exists() )
		{
			res = locked_f( f );
		}
		return res;
	}

	private void write_string( final String fn, final String s )
	{
		try
		{
			BufferedWriter w = new BufferedWriter( new FileWriter( fn) );
	    	w.write( s );
         	w.close();
		}
	    catch( Exception e )
	    {
	    }
	}

	private void write_busy()
	{
		write_string( lockfn, "busy\n" );
	}

	private void write_done()
	{
		write_string( lockfn, "done\n" );
	}

    public boolean present()
    {
        File f = new File( local );
        return ( f.exists() && f.length() > 0 && !locked() );
    }

	public Long fileSize()
	{
        File f = new File( local );
        return f.length();
	}

	private int delete_file( final String fn, final BLAST_LOG_SETTING log )
	{
		int res = 0;
		File f = new File( fn );
		if ( f.delete() )
		{
			BLAST_SEND.send( log, String.format( "'%s' deleted", fn ) );
			res = 1;
		}
		else
		{
			BLAST_SEND.send( log, String.format( "'%s' delete failed", fn ) );
		}
		return res;
	}

	public int clean( final BLAST_LOG_SETTING log )
	{
		int res = delete_file( local, log );
		delete_file( lockfn, log );
		return res;
	}

	public int MD5Check( final BLAST_LOG_SETTING log )
	{
		int res = 0;
		MD5ForFile cmd5 = new MD5ForFile( local );
		String digest = cmd5.getMD5();

		byte[] b64 = Base64.getEncoder().encode( digest.getBytes() );
		String b64digest = new String( b64 );

		String msg = String.format( "MD5 for '%s'\n", name );
		msg += String.format( "MD5 stored  : %s\n", md5 );
		msg += String.format( "MD5 computed: %s\n", b64digest );
		BLAST_SEND.send( log, msg );

		if ( md5.equals( digest ) ) res = 1;
		return res;
	}

    public int copy( Storage storage, final String bucket, final BLAST_LOG_SETTING log )
    {
        int res = 0;
		if ( !locked() )
		{
		    String dir = local.substring( 0, local.lastIndexOf( File.separator) );
			write_busy();
		    try
		    {
		        Files.createDirectories( Paths.get( dir ) );
				if ( log.db_copy )
					BLAST_SEND.send( log, String.format( "copy: '%s'", local ) );
		        res = BLAST_GS_DOWNLOADER.download( storage, bucket, name, local ) ? 1 : 0;
		    }
		    catch( Exception e )
		    {
		    }
			write_done();
		}
        return res;
    }
}

