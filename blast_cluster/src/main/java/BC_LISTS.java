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

import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

class BC_LIST extends Thread
{
    protected final BC_CONTEXT context;
    protected final String srcName;
	protected final int limit;
	protected final PrintStream ps;
	protected int line_nr = 0;
    private final AtomicBoolean running;

    public BC_LIST( final BC_CONTEXT a_context, final String a_srcName, final PrintStream a_ps, int a_limit )
    {
        context = a_context;
		srcName = a_srcName;
		limit = a_limit;
		ps = a_ps;
		line_nr = 0;
		running = new AtomicBoolean( true );
    }

	protected boolean loop()
	{
		boolean res = ( context.is_running() && running.get() );
		if ( res && limit > 0 ) res = ( line_nr <= limit );
		return res;
	}

	protected void stopList()
	{
		running.set( false );
	}

	protected void inc_line()
	{
		line_nr += 1;
	}

    protected void sleepFor( long milliseconds )
    {
        try { Thread.sleep( milliseconds ); }
        catch ( InterruptedException e ) { }
    }

	protected void submitFile( String request_filename )
	{
		boolean done = false;
		while( context.is_running() && !done )
		{
			int res = context.add_request_file( request_filename, ps );
			done = ( res != 0 );	/* 0...not done( because no space in queue, 1...done, -1...invalid */
			if ( context.is_running() && !done )
		        sleepFor( 250 );
		}
		inc_line();
	}
}

class BC_FILE_LIST extends BC_LIST
{
    public BC_FILE_LIST( final BC_CONTEXT a_context, final String a_srcName, final PrintStream a_ps, int a_limit )
    {
		super( a_context, a_srcName, a_ps, a_limit );
    }

    @Override public void run()
	{
		ps.printf( String.format( "request-list '%s' start\n", srcName ) );
		try
		{
		    FileInputStream fs = new FileInputStream( srcName );
		    BufferedReader br = new BufferedReader( new InputStreamReader( fs ) );
		    String line;
		    String src = "";

            while ( loop() && ( ( line = br.readLine() ) != null ) )
            {
                if ( !line.isEmpty() && !line.startsWith( "#" ) )
                {
                    if ( line.startsWith( ":src=" ) )
                    {
                        src = line.trim().substring( 5 );
                    }
                    else
                    {
                        if ( !src.isEmpty() )
                            submitFile( String.format( "%s/%s", src, line.trim() ) );
						else
							submitFile( line.trim() );
                    }
                }
            }
            br.close();
		}
        catch( Exception e )
        {
            ps.printf( String.format( "request-list '%s' : %s", srcName, e ) );
        }
		ps.printf( String.format( "request-list '%s' done\n", srcName ) );
	}
}

class BC_BUCKET_LIST extends BC_LIST
{
    public BC_BUCKET_LIST( final BC_CONTEXT a_context, final String a_srcName, final PrintStream a_ps, int a_limit )
    {
		super( a_context, a_srcName, a_ps, a_limit );
    }

    @Override public void run()
	{
		ps.printf( String.format( "bucket-list '%s' start\n", srcName ) );

		List< String > files = BC_GCP_TOOLS.list( srcName );
		Iterator< String > iter = files.iterator();
		
        while ( loop() && iter.hasNext() )
		{
			String fn = iter.next();
			if ( fn.endsWith( "json" ) )
				submitFile( String.format( "%s/%s", srcName, fn ) );
		}

		ps.printf( String.format( "bucket-list '%s' done\n", srcName ) );
	}
}

public class BC_LISTS
{
    private final BC_CONTEXT context;
    private final List< BC_LIST > lists;

    public BC_LISTS( final BC_CONTEXT a_context )
    {
		context = a_context;
        lists = new ArrayList<>();
    }

	private void join_list( BC_LIST list )
	{
	    try { list.join(); }
	    catch( InterruptedException e ) { }
	}

	private void join_done_list()
	{
        for ( BC_LIST list : lists )
		{
			if ( list.getState() == Thread.State.TERMINATED )
				join_list( list );
		}
	}

	public void addFile( final String filename, final PrintStream ps, int limit )
	{
		/* try to join lists that are done */
		join_done_list();

		/* create a new list, and start it */
		BC_LIST list = new BC_FILE_LIST( context, filename, ps, limit );
        lists.add( list );
		list.start();
	}

	public void addBucket( final String filename, final PrintStream ps, int limit )
	{
		/* try to join lists that are done */
		join_done_list();

		/* create a new list, and start it */
		BC_LIST list = new BC_BUCKET_LIST( context, filename, ps, limit );
        lists.add( list );
		list.start();
	}

    public void join()
    {
        for ( BC_LIST list : lists )
		    join_list( list );
    }

	public void stop()
	{
        for ( BC_LIST list : lists )
		    list.stopList();
	}
}

