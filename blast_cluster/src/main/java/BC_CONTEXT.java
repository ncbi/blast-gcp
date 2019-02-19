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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BC_CONTEXT
{
	private final BC_SETTINGS settings;
    private final AtomicBoolean running;
    private final ConcurrentLinkedQueue< BC_COMMAND > command_queue;
    private final ConcurrentLinkedQueue< BC_REQUEST > request_queue;

    public BC_CONTEXT( final BC_SETTINGS a_settings )
	{
		settings = a_settings;
        running = new AtomicBoolean( true );
        command_queue = new ConcurrentLinkedQueue<>();
        request_queue = new ConcurrentLinkedQueue<>();
	}

    public int can_take() { return ( settings.req_max_backlog - request_queue.size() ); }

    public boolean is_running() { return running.get(); }
    public void stop() { running.set( false ); }

    public void push_command( final BC_COMMAND command ) { command_queue.offer( command ); }
    public BC_COMMAND pull_cmd() { return command_queue.poll(); }

	public BC_SETTINGS get_settings() { return settings; }


    public boolean contains( BC_REQUEST request )
    {
        boolean res = request_queue.contains( request );
        //if ( !res ) res = is_a_running_id( request.id ); /* we will need this protection again */
        return res;
    }

    public boolean add_request( BC_REQUEST request, final PrintStream ps )
    {
        boolean res = !contains( request );
        if ( res )
        {
            request_queue.offer( request );

            if ( ps != null && settings.debug.req_added )
            	ps.printf( "REQUEST '%s' added to queue\n", request.id );
        }
        else
        {
            if ( ps != null )
                ps.printf( "REQUEST '%s' rejected: already queued\n", request.id );
        }
        return res;
    }

    public boolean add_request_string( final String req_string, final PrintStream ps )
    {
        boolean res = false;
        if ( can_take() > 0 )
        {
            BC_REQUEST request = BC_REQUEST_READER.parse_from_string( req_string );
            if ( request != null )
                res = add_request( request, ps );
            else if ( ps != null )
                ps.printf( "invalid request '%s'\n", req_string );
        }
        return res;
    }

    public boolean add_request_file( final String filename, final PrintStream ps )
    {
        boolean res = false;
        if ( can_take() > 0 )
        {
            BC_REQUEST request = BC_REQUEST_READER.parse_from_file( filename );
            if ( request != null )
            	res = add_request( request, ps );
            else if ( ps != null )
                ps.printf( "invalid request in file '%s'\n", filename );
        }
        return res;
    }

}
