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
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetAddress;

import org.apache.spark.SparkEnv;

/**
 * utility-class to send debug-information from workers to the master
 * implemented as a singleton
 *
*/
public class BC_SEND implements Serializable
{
    private static BC_SEND instance = null;

    private PrintStream ps;
    private String localName;
    private String executorName;
	//private long start_milisecs;

/**
 * private constructor to prevent accidential instantiation
 *
 * @param	host 	the host the send messages to
 * @param	port 	the port on the host the send messages to
*/
    private BC_SEND( final String host, final int port )
    {
        try
        {
			localName = java.net.InetAddress.getLocalHost().getHostName();
			executorName = SparkEnv.get().executorId();
            Socket socket = new Socket( host, port );
            socket.setTcpNoDelay( true );
            ps = new PrintStream( socket.getOutputStream() );
        }
        catch ( Exception e )
        {
            localName = "unknown";
            ps = null;
        }
    }

/**
 * private helper method to return already existing or newly created instance
 *
 * @param	host 	the host the send messages to
 * @param	port 	the port on the host the send messages to
*/
    public static BC_SEND getInstance( final String host, final int port )
    {
        if ( instance == null )
            instance = new BC_SEND( host, port );
        else if ( instance.ps == null )
            instance = new BC_SEND( host, port );
        return instance;
    }

/**
 * private helper method to send message, inserts local name and executorName
 *
 * @param	msg		message to send
*/
    private void send_msg( final String msg )
    {
        try
        {
            if ( ps != null )
			{
				/*
				long milliseconds = System.currentTimeMillis();
				ps.printf( "%d %s %s %s\n", milliseconds, localName, executorName, msg );
				*/
				ps.printf( "%s %s %s\n", localName, executorName, msg );
			}
        }
        catch ( Exception e )
        {
            ps = null;
        }
    }

/**
 * public method to send message, to host:port
 *
 * @param	host 	the host the send messages to
 * @param	port 	the port on the host the send messages to
 * @param	msg		message to send
*/
    public static void send( final String host, final int port, final String msg )
    {
        BC_SEND inst = getInstance( host, port );
        if ( inst != null )
            inst.send_msg( msg );
    }

/**
 * public method to send message, to host,port taken from debug-settings-instance
 *
 * @param	debug 	instance BC_DEBUG_SETTINGS to take host/port from
 * @param	msg		message to send
*/
    public static void send( final BC_DEBUG_SETTINGS debug, final String msg )
    {
        send( debug.host, debug.port, msg );
    }

}

