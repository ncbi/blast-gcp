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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.concurrent.atomic.AtomicBoolean;

class BLAST_LIST_SUBMIT extends Thread
{
    private final BLAST_STATUS status;
    private final CMD_Q_ENTRY cmd;
	private final String list_filename;
	private final String list_filter;
    private final AtomicBoolean running;
	private final boolean exit_on_end;
	private final boolean print_request_added;

    public BLAST_LIST_SUBMIT( BLAST_STATUS a_status,
                              CMD_Q_ENTRY a_cmd,
							  boolean print_request_added )
    {
        this.status = a_status;
        this.cmd = a_cmd;
		this.print_request_added = print_request_added;
        this.running = new AtomicBoolean( true );

		String[] cmd_parts = this.cmd.line.substring( 1 ).split( " " );
		if ( cmd_parts.length > 0 )
		{
			this.list_filename = cmd_parts[ 0 ];

			if ( cmd_parts.length > 1 )
			{
				if ( cmd_parts[ 1 ].equals( "exit" ) )
				{
					this.exit_on_end = true;
					if ( cmd_parts.length > 2 )
						this.list_filter = cmd_parts[ 2 ];
					else
						this.list_filter = "";
				}
				else
				{
					this.list_filter = cmd_parts[ 1 ];
					if ( cmd_parts.length > 2 )
						this.exit_on_end = cmd_parts[ 2 ].equals( "exit" );
					else
						this.exit_on_end = false;
				}
			}
			else
			{
				this.list_filter = "";
				this.exit_on_end = false;
			}
		}
		else
		{
			this.list_filename = "";
			this.list_filter = "";
			this.exit_on_end = false;
		}
    }

    private  void sleep_now( Integer ms )
    {
        try
        {
            Thread.sleep( ms );
        }
        catch ( InterruptedException e )
        {
        }
    }

    public boolean is_running() { return running.get(); }
    public void done() { running.set( false ); }

    @Override public void run()
    {
        try
        {
            cmd.stream.println( String.format( "starting list : '%s', filter:'%s'", list_filename, list_filter ) );
            FileInputStream fs = new FileInputStream( list_filename );
            BufferedReader br = new BufferedReader( new InputStreamReader( fs ) );
            String line;
            String src = "";
            long started_at = System.currentTimeMillis();
            long total_at_start = status.get_total_requests();
            long submitted = 0L;

            while ( is_running() && ( ( line = br.readLine() ) != null ) )
            {
                if ( !line.isEmpty() && !line.startsWith( "#" ) )
                {
                    if ( line.startsWith( ":src=" ) )
                    {
                        src = line.trim().substring( 5 );
                    }
                    else
                    {
                        String req_file = line.trim();;
                        if ( !src.isEmpty() )
                            req_file = String.format( "%s/%s", src, line.trim() );
                        boolean done = false;
                        while( !done )
                        {
							int res = status.add_request_file( req_file, cmd.stream, list_filter );
							if ( res == 0 )
                                sleep_now( 500 );
							else
							{
								done = true;
								if ( res == 1 )
								{
									if ( print_request_added )
                        				cmd.stream.println( String.format( "REQUEST-FILE '%s' added", req_file ) );
                        			submitted++;
								}
							}
                        }
                    }
                }
            }
            br.close();

            // for testing wait for the status to have no more jobs...
            while( ( status.get_total_requests() - total_at_start ) < submitted )
                sleep_now( 100 );

            long elapsed = System.currentTimeMillis() - started_at;
            cmd.stream.println( String.format( "done with list : %s in %,d ms", list_filename, elapsed ) );

			if ( exit_on_end )
			{
				cmd.stream.println( "exit at end of list." );
				status.stop();
			}
        }
        catch( Exception e )
        {
            cmd.stream.println( String.format( "BLAST_LIST_SUBMIT : %s", e ) );
        }
        done();
    }    
}
