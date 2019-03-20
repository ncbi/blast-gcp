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

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

/**
 * Infrastructure-Class to display lines, read form a tcp-socket for debugging
 * class is used only by the BC_DEBUG_RECEIVER-class
 * - stores reference to application-context
 * - stores socket to use
 * @see        BC_CONTEXT
*/
class BC_DEBUG_RECEIVER_CLIENT extends Thread
{
    private final BC_CONTEXT context;
    private final long sleep_time;
    private final Socket socket;
    private final Logger logger;

/**
 * create instance of BC_DEBUG_RECEIVER_CLIENT
 * - store reference application-context
 * - store socket to use
 *
 * @param a_context    application-context
 * @param a_socket     socket to use
 * @see        BC_CONTEXT
*/
    public BC_DEBUG_RECEIVER_CLIENT( BC_CONTEXT a_context, Socket a_socket )
    {
        context = a_context;
        sleep_time = context.get_settings().debug_receiver_sleep_time;
        socket = a_socket;
        logger = LogManager.getLogger( BC_DEBUG_RECEIVER_CLIENT.class );
    }

/**
 * overwritten run method of Thread-BC_DEBUG_RECEIVER_CLIENT
 * - create a buffered-reader for reading from a socket
 * - loop until application closed
 * - if the reader is ready to produce a line, read the line,
 *   print the line on the system-console
 *
 * @see        BC_CONTEXT
*/
    @Override public void run()
    {
        try
        {
            socket.setTcpNoDelay( true );
            BufferedReader br = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
            String input;

            while( context.is_running() && br != null )
            {
                String line = null;
                try
                {
                    if ( br.ready() )
                        line = br.readLine().trim();
                }
                catch ( IOException e ) { }

                if ( line != null && !line.isEmpty() )
                {
                    logger.info( line.trim() );
                }
                else if ( context.is_running() )
                {
                    try
                    {
                        Thread.sleep( sleep_time );
                    }
                    catch ( InterruptedException e ) { }
                }
            }
            socket.close();
        }
        catch ( Exception e )
        {
            logger.info( String.format( "BC_DEBUG_RECEIVER_CLIENT: %s", e ) );
        }
    }
}

/**
 * Infrastructure-Class to listen on the debug-socket for connections coming from workers
 * - stores reference to application-context
 * - owns list of BC_DEBUG_RECEIVER_CLIENT-instances
 *
 * @see        BC_CONTEXT
*/
public final class BC_DEBUG_RECEIVER extends Thread
{
    private final BC_CONTEXT context;
    private List< BC_DEBUG_RECEIVER_CLIENT > clients;
    private final Logger logger;

/**
 * create instance of BC_DEBUG_RECEIVER
 * - store reference application-context
 * - creates empty list for client-connections from the workers
 *
 * @param a_context    application-context
 * @see        BC_CONTEXT
*/
    public BC_DEBUG_RECEIVER( BC_CONTEXT a_context )
    {
        context = a_context;
        clients = new ArrayList<>();
        logger = LogManager.getLogger( BC_DEBUG_RECEIVER.class );
    }

/**
 * overwritten run method of Thread-BC_DEBUG_RECEIVER
 * - create a serversocket to listen on the debug-port
 * - loop until application closed
 * - if a new connection has been accepted, create a instance of BC_DEBUG_RECEIVER_CLIENT,
 *   add it to the clients-list, start the client thread
 *
 * @see        BC_CONTEXT
*/
    @Override public void run()
    {
        try
        {
            int port = context.get_settings().debug.port;
            logger.info( String.format( "DEBUG_RECEIVER listening on port: %d", port ) );

            ServerSocket ss = new ServerSocket( port );
            while( context.is_running() )
            {
                try
                {
                    ss.setSoTimeout( 500 );
                    Socket client_socket = ss.accept();
                    BC_DEBUG_RECEIVER_CLIENT client = new BC_DEBUG_RECEIVER_CLIENT( context, client_socket );
                    clients.add( client );
                    client.start();
                }
                catch ( Exception e )
                {
                    // do nothing if the loop times out...
                }
            }
            ss.close();
        }
        catch ( Exception e )
        {
            logger.info( String.format( "BC_DEBUG_RECEIVER: %s", e ) );
        }
    }

/**
 * wait for all client-socket-threads to finish, then wait for this tread to finish
 *
*/
    public void join_clients()
    {
        for( BC_DEBUG_RECEIVER_CLIENT client : clients )
        {
            try { client.join(); }
            catch( InterruptedException e ) { }
        }
        try { join(); }
        catch( InterruptedException e ) { }
    }
}

