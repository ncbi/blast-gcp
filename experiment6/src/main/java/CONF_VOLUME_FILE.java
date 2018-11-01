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

import java.nio.file.Paths;
import java.nio.file.Files;

import com.google.api.services.storage.Storage;

public class CONF_VOLUME_FILE implements Serializable
{
    public String f_type;   /* for example : 'nsq' */
    public String f_name;   /* for example : 'nt_50M.00.nsq' */
    public String f_md5;    /* for example : 'DBEis3HM8wceV0Pno3TPlQ==' */
    public String f_local;  /* for example : '/tmp/blast/db/nt_50M.00/nt_50M.00.nsq' */


    @Override public String toString()
    {
        return String.format( "\t\t\t[ type:'%s', name:'%s', md5:'%s', local:'%s'\n", f_type, f_name, f_md5, f_local );
    }

	private String lockFileName()
	{
		return String.format( "%s.lock", f_local );
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
		File f = new File( lockFileName() );
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
		write_string( lockFileName(), "busy\n" );
	}

	private void write_done()
	{
		write_string( lockFileName(), "done\n" );
	}

    public boolean present()
    {
        File f = new File( f_local );
        return ( f.exists() && f.length() > 0 && !locked() );
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
		int res = delete_file( f_local, log );
		delete_file( lockFileName(), log );
		return res;
	}

    public int copy( Storage storage, final String bucket, final BLAST_LOG_SETTING log )
    {
        int res = 0;
		if ( !locked() )
		{
		    String dir = f_local.substring( 0, f_local.lastIndexOf( File.separator) );
			write_busy();
		    try
		    {
		        Files.createDirectories( Paths.get( dir ) );
				if ( log.db_copy )
					BLAST_SEND.send( log, String.format( "copy: '%s'", f_local ) );				
		        res = BLAST_GS_DOWNLOADER.download( storage, bucket, f_name, f_local ) ? 1 : 0;
		    }
		    catch( Exception e )
		    {
		    }
			write_done();
		}
        return res;
    }
}

