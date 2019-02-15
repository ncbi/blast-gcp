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

import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetAddress;

import org.apache.spark.SparkEnv;

public class BLAST_SEND
{
    private static BLAST_SEND instance = null;

    private PrintStream ps;
    private String localName;
    private String executorName;
	private long start_milisecs;

    private BLAST_SEND( final String host, final int port )
    {
        try
        {
            localName = "W" + java.net.InetAddress.getLocalHost().getHostName().replaceAll( "\\D+","" );
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

    public static BLAST_SEND getInstance( final String host, final int port )
    {
        if ( instance == null )
        {
            instance = new BLAST_SEND( host, port );
        }
        else
        {
            if ( instance.ps == null )
                instance = new BLAST_SEND( host, port );
        }
        return instance;
    }

    private void send_msg( final String msg )
    {
        try
        {
            if ( ps != null )
			{
				long milliseconds = System.currentTimeMillis();
				ps.printf( "%d %s %s %s\n", milliseconds, localName, executorName, msg );
			}
        }
        catch ( Exception e )
        {
            ps = null;
        }
    }

    public static void send( final String host, final int port, final String msg )
    {
        BLAST_SEND inst = getInstance( host, port );
        if ( inst != null )
            inst.send_msg( msg );
    }

    public static void send( final BLAST_LOG_SETTING log, final String msg )
    {
        send( log.host, log.port, msg );
    }

    public static String resolve( final String hostname )
    {
        try
        {
            InetAddress address = InetAddress.getByName( hostname ); 
            return address.getHostAddress();
        }
        catch ( Exception e )
        {
            return hostname;
        }
    }
}

