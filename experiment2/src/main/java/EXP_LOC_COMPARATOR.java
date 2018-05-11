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

package gov.nih.nlm.ncbi.exp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;

public class EXP_LOC_COMPARATOR implements Comparator< String >
{
    private InetAddress hostname;

    
    // Default constructor. Automatically tries to resolve the name of the local machine.
    public EXP_LOC_COMPARATOR()
    {
        try
        {
            this.hostname = InetAddress.getLocalHost();
        }
        catch ( UnknownHostException e )
        {
            this.hostname = null;
        }
    }

    // Constucts a comparator using as the name of the local machine the hostname provided.
    // Parameters:
    // hostname the host name of the current machine.
    public EXP_LOC_COMPARATOR( String hostname )
    {
        try
        {
            this.hostname = InetAddress.getByName( hostname );
        }
        catch ( UnknownHostException e )
        {
            this.hostname = null;
        }
    }

    @Override public int compare( String loc1, String loc2 )
    {
        int result = 0;
        try
        {
            InetAddress addr1 = InetAddress.getByName( loc1 );
            InetAddress addr2 = InetAddress.getByName( loc2 );

            if ( addr1 == addr2 )
            {
                result = 0;
            }
            else if ( addr1.isLoopbackAddress() )
            {
                result = -1;
            }
            else if ( addr2.isLoopbackAddress() )
            {
                result = 1;
            }
            else if ( hostname != null )
            {
                if ( addr1.getHostAddress().equals( hostname.getHostAddress() ) )
                {
                    result = -1;
                }
                else if ( addr2.getHostAddress().equals( hostname.getHostAddress() ) )
                {
                    result = 1;
                }
            }
        }
        catch ( UnknownHostException e )
        {

        }
        return result;
    }

    
    // Returns:
    // the host name of the local machine.
    public InetAddress getHostname()
    {
        return hostname;
    }
}


