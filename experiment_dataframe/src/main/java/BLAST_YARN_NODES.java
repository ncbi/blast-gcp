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

import java.io.InputStreamReader;
import java.io.BufferedReader;

public class BLAST_YARN_NODES
{
    public final List< String > nodes;

    public BLAST_YARN_NODES()
    {
        nodes = new ArrayList<>();
        discover();
    }

    private final void discover()
    {
        try
        {
            Runtime r = Runtime.getRuntime();
            Process p = r.exec( "yarn node -list -all" );
            p.waitFor();
            InputStreamReader isr = new InputStreamReader( p.getInputStream() );
            BufferedReader b = new BufferedReader( isr );
            String line = "";
            while ( ( line = b.readLine() ) != null )
            {
                String l = line.trim();
                if ( !l.startsWith( "Total Nodes" ) && !l.startsWith( "Node-Id" ) )
                {
                    nodes.add( l.substring( 0, l.indexOf( ':' ) ) );
                }
            }
            b.close();
        }
        catch ( Exception e )
        {
            System.out.println( e );
        }
    }

    public final Integer count()
    {
        try
        {
            return nodes.size();
        }
        catch ( Exception e )
        {
        }
        return 0;
    }

    public String getHost( int idx )
    {
        try
        {
            return nodes.get( idx );
        }
        catch ( Exception e )
        {
        }
        return "";
    }

    @Override public String toString()
    {
        String S = "BLAST_YARN_NODES:";
        for ( String item : nodes )
            S = S + String.format( "\nnode: '%s'", item );
        return S;
    }
}
