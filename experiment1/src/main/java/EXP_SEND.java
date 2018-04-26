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
package gov.nih.nlm.ncbi.exp1;

import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetAddress;

import org.apache.spark.SparkEnv;

public class EXP_SEND
{
    private static EXP_SEND instance = null;

    private final String host;
    private final int port;
    private String localName;

    private EXP_SEND( final String host, final int port )
    {
        this.host = host;
        this.port = port;
        try
        {
            InetAddress localMachine = java.net.InetAddress.getLocalHost();
            localName = localMachine.getHostName();
        }
        catch ( Exception e )
        {
            localName = "unknown";
        }
    }

    public static EXP_SEND getInstance( final String host, final int port )
    {
        if ( instance == null )
        {
            instance = new EXP_SEND( host, port );
        }
        return instance;
    }

    private void send_msg( final String msg )
    {
        try
        {
            Socket socket = new Socket( host, port );
            PrintStream ps = new PrintStream( socket.getOutputStream() );
            ps.printf( "[%s|%s] %s\n", localName, SparkEnv.get().executorId(), msg );
            socket.close();
        
        }
        catch ( Exception e )
        {
        }
    }

    public static void send( final String host, final int port, final String msg )
    {
        EXP_SEND inst = getInstance( host, port );
        if ( inst != null )
            inst.send_msg( msg );
    }

    public static void send( final EXP_SETTINGS bls, final String msg )
    {
        send( bls.log_host, bls.log_port, msg );
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

