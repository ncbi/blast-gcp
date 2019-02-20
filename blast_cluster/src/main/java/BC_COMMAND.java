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
package gov.nih.nlm.ncbi.blast_spark_cluster;

import java.io.PrintStream;

public class BC_COMMAND
{
    private final PrintStream stream;
    private final String line;

    BC_COMMAND( final PrintStream a_stream, final String a_line )
    {
        stream = a_stream;
        line = a_line;
    }

	public boolean is_exit() { return line.equals( "exit" ); }
	public boolean is_file_request() { return line.startsWith( "F" ); }
	public boolean is_list_request() { return line.startsWith( "L" ); }

	public void handle( BC_CONTEXT context )
	{
        if ( is_exit() ) context.stop();
		else if ( is_file_request() ) context.add_request_file( line.substring( 1 ).trim(), stream );

	}
}

