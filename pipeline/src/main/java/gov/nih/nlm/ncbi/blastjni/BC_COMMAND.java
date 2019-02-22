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

public class BC_COMMAND
{
    private final PrintStream stream;
    private String[] parts;
    private final int num_parts;

    BC_COMMAND( final PrintStream a_stream, final String line )
    {
        stream = a_stream;
		parts = line.trim().split( "\\s+" );	/* split on whitespace */
		num_parts = parts.length;
    }

	public boolean is_exit() { return parts[ 0 ].equals( "exit" ); }
	public boolean is_stop() { return parts[ 0 ].equals( "stop" ); }
	public boolean is_file_request() { return parts[ 0 ].equals( "F" ); }
	public boolean is_list_request() { return parts[ 0 ].equals( "L" ); }
	public boolean is_bucket_request() { return parts[ 0 ].equals( "B" ); }

	public int toInt( String s )
	{
		int res = 0;
		try	{ res = Integer.parseInt( s ); }
		catch ( NumberFormatException e ) { res = 0; }
		return res;
	}

	public void handle_file_request( BC_CONTEXT context )
	{
		if ( num_parts > 1 )
			context.add_request_file( parts[ 1 ], stream );			
	}

	public void handle_list_request( BC_CONTEXT context )
	{
		if ( num_parts > 1 )
		{
			if ( num_parts > 2 )
				context.addRequestList( parts[ 1 ], stream, toInt( parts[ 2 ] ) );
			else
				context.addRequestList( parts[ 1 ], stream, 0 );
		}
	}

	public void handle_bucket_request( BC_CONTEXT context )
	{
		if ( num_parts > 1 )
		{
			if ( num_parts > 2 )
				context.addRequestBucket( parts[ 1 ], stream, toInt( parts[ 2 ] ) );
			else
				context.addRequestBucket( parts[ 1 ], stream, 0 );
		}
	}

	public void handle( BC_CONTEXT context )
	{
        if ( is_exit() ) context.stop();
        else if ( is_stop() ) context.stop_lists();
		else if ( is_file_request() ) handle_file_request( context );
		else if ( is_list_request() ) handle_list_request( context );
		else if ( is_bucket_request() ) handle_bucket_request( context );
	}
}

