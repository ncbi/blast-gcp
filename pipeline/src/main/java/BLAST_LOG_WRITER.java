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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class BLAST_LOG_WRITER extends Thread
{
    private final BLAST_STATUS status;
    private final ConcurrentLinkedQueue< String > q;
    private final AtomicBoolean console;
    private final AtomicBoolean filemode;
    private final AtomicBoolean filemode_changed;
	private String filename;

    public BLAST_LOG_WRITER( BLAST_STATUS a_status )
    {
        this.status = a_status;
		this.q = new ConcurrentLinkedQueue<>();
        this.console = new AtomicBoolean( false );
        this.filemode = new AtomicBoolean( false );
        this.filemode_changed = new AtomicBoolean( false );
		this.filename = "";
    }
    
	public void write( final String msg ) { q.offer( msg ); }
	public void console_out( final Boolean enabled ) { console.set( enabled ); }
	public Boolean console_status() { return console.get(); }

	public void set_filename( final String a_filename )
	{
		if ( a_filename == null )
		{
			filename = "";
			filemode_changed.set( true );
		}
		else if ( !filename.equals( a_filename ) )
		{
        	filename = a_filename;
			filemode_changed.set( true );
		}
	}

	public String get_filename() { return filename; }

    @Override public void run()
    {
        try
        {
			BufferedWriter writer = null;

            while( status.is_running() ) 
            {
				String msg = q.poll();
				if ( msg != null )
				{
					if ( console.get() )
						System.out.println( String.format( "-->%s", msg ) );
					if ( filemode.get() )
					{
						writer.write( String.format( "%s\n", msg ) );
						writer.flush();
					}
				}
				else if ( filemode_changed.get() )
				{
					if ( writer != null )
					{
						writer.close();
						writer = null;
					}

					if ( !filename.isEmpty() )
					{
						writer = new BufferedWriter( new FileWriter( new File( filename ) ) );
					}
					filemode.set( writer != null );
					filemode_changed.set( false );
				}
				else
				{
					Thread.sleep( 50 );
				}
            }
        }
        catch ( Exception e )
        {
            System.out.println( String.format( "BLAST_LOG_WRITER: %s", e ) );
        }
    }
}

