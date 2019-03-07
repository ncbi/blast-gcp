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

/**
 * command-class
 * - has reference to store the origin of the command
*/
public final class BC_COMMAND
{
    private final PrintStream origin_stream;
    private String[] parts;
    private final int num_parts;

/**
 * create instance of BC_COMMAND from a String
 * - store the origin of the command
 * - preparse the command by splitting on whitespace
 *
 * @param origin console or tcp-socket where the command originated
 * @param line   String obtained from console or tcp-socket
*/
    BC_COMMAND( final PrintStream origin, final String line )
    {
        origin_stream = origin;
        parts = line.trim().split( "\\s+" );    /* split on whitespace */
        num_parts = parts.length;
    }

/**
 * test for exit command ( exit the application )
 *
 * @return     is it a exit command ?
*/
    private boolean is_exit() { return parts[ 0 ].equals( "exit" ); }

/**
 * test for stop command ( stop processing pending lists )
 *
 * @return     is it a stop command ?
*/
    private boolean is_stop() { return parts[ 0 ].equals( "stop" ); }

/**
 * test for file command ( process a single request-file )
 *
 * @return     is it a file command ?
*/
    private boolean is_file_request() { return parts[ 0 ].equals( "F" ); }

/**
 * test for list command ( process a list of request-files )
 *
 * @return     is it a list command ?
*/
    private boolean is_list_request() { return parts[ 0 ].equals( "L" ); }

/**
 * test for bucket command ( process a bucket of request-files )
 *
 * @return     is it a bucket command ?
*/
    private boolean is_bucket_request() { return parts[ 0 ].equals( "B" ); }

/**
 * test for info command ( print request-queue-size and running commands )
 *
 * @return     is it a info command ?
*/
    private boolean is_info_request() { return parts[ 0 ].equals( "I" ); }

/**
 * test for history command ( print last n commands )
 *
 * @return     is it a history command ?
*/
    private boolean is_history_request() { return parts[ 0 ].equals( "H" ); }

/**
 * test for execute from history command ( executes commnd from history )
 *
 * @return     is it a execute from history command ?
*/
    private boolean is_execute_request() { return parts[ 0 ].equals( "E" ); }

/**
 * helper-function to convert String to int
 *
 * @param s  String to be converted to int
 * @return result of conversion
*/
    private int toInt( String s )
    {
        int res = 0;
        try { res = Integer.parseInt( s ); }
        catch ( NumberFormatException e ) { res = 0; }
        return res;
    }

/**
 * handle the file-command, by delegating it to the global context
 *
 * @param context the global application-context
 * @see        BC_CONTEXT
*/
    private void handle_file_request( BC_CONTEXT context )
    {
        if ( num_parts > 1 )
            context.add_request_file( parts[ 1 ], origin_stream );
        else
            origin_stream.printf( "filename is missing\n" );
    }

/**
 * handle the list-command, by delegating it to the global context
 *
 * @param context the global application-context
 * @see        BC_CONTEXT
*/
    private void handle_list_request( BC_CONTEXT context )
    {
        if ( num_parts > 1 )
        {
            int limit = ( num_parts > 2 ) ? toInt( parts[ 2 ] ) : 0;
            context.addRequestList( parts[ 1 ], origin_stream, limit );
        }
        else
            origin_stream.printf( "filename is missing\n" );
    }

/**
 * handle the bucket-command, by delegating it to the global context
 *
 * @param context the global application-context
 * @see        BC_CONTEXT
*/
    private void handle_bucket_request( BC_CONTEXT context )
    {
        if ( num_parts > 1 )
        {
            int limit = ( num_parts > 2 ) ? toInt( parts[ 2 ] ) : 0;
            context.addRequestBucket( parts[ 1 ], origin_stream, limit );
        }
        else
            origin_stream.printf( "bucket-url is missing\n" );
    }

/**
 * handle the history-command
 *
 * @param context the global application-context
 * @see        BC_CONTEXT
*/
    private void handle_history_request( BC_CONTEXT context )
    {
        int entries = context.get_cmd_history();
        if ( entries > 0 )
        {
            int limit = ( num_parts > 1 ) ? toInt( parts[ 1 ] ) : 0;
            int high = ( limit > 0 ) ? limit : entries;
            for( int idx = 0; idx < high; idx = idx + 1 )
            {
                String s = context.get_cmd_history( idx );
                if ( !s.isEmpty() )
                {
                    origin_stream.printf( String.format( "[%d] %s\n", idx, s ) );
                }
            }
        }
    }

/**
 * handle the execute from history-command
 *
 * @param context the global application-context
 * @see        BC_CONTEXT
*/
    private void handle_execute_request( BC_CONTEXT context )
    {
        int entries = context.get_cmd_history();
        if ( entries > 0 )
        {
            int idx = ( num_parts > 1 ) ? toInt( parts[ 1 ] ) : 0;
            String s = context.get_cmd_history( idx );
            if ( !s.isEmpty() )
            {
                origin_stream.printf( String.format( "%s\n", s ) );
                context.push_command( origin_stream, s );
            }
        }
    }

/**
 * handle a preparsed command using the given context
 *
 * @param context application-context needed to handle the command
 * @see        BC_CONTEXT
*/
    public void handle( BC_CONTEXT context )
    {
        if ( is_exit() ) context.stop();
        else if ( is_stop() ) context.stop_lists();
        else if ( is_file_request() ) handle_file_request( context );
        else if ( is_list_request() ) handle_list_request( context );
        else if ( is_bucket_request() ) handle_bucket_request( context );
        else if ( is_history_request() ) handle_history_request( context );
        else if ( is_execute_request() ) handle_execute_request( context );
        else if ( is_info_request() ) context.print_info( origin_stream );
        else origin_stream.printf( "unknown: %s\n", parts );
    }
}

