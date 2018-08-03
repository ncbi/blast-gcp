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

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.io.FileOutputStream;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;

class tb_list implements Serializable
{
	public String protocol;
	public int top_n;
	public tb_res[] results;

    public tb_list( final String src, int top_n )
    {
		this.top_n = top_n;
        JsonParser parser = new JsonParser();
        try
        {
			JsonElement tree = parser.parse( src.trim() );
            JsonObject root  = tree.getAsJsonObject();

	        this.protocol = SE_UTILS.get_json_string( root, "protocol", "" );
			try
			{
				JsonArray a = root.getAsJsonArray( "blast_tb_list" );
				int n = a.size();
				this.results = new tb_res[ n ];
				for ( int i = 0; i < n; i++ )
					this.results[ i ] = new tb_res( a.get( i ) );
			}
		    catch( Exception e )
		    {
		        this.results = null;
		    }
		}
        catch( Exception e )
        {
            System.out.println( String.format( "tb_list(1) : %s", e ) );
        }
    }

    public tb_list( final tb_list l1, tb_list l2 )
    {
		this.protocol = l1.protocol;
		this.top_n = l1.top_n;
        merge_sort( l1, l2 );
    }

	private tb_res get( int idx )
	{
		int l = count();
		if ( l > 0 && idx < l )
			return results[ idx ];
		return null;
	}

    private void merge_sort( tb_list l1, tb_list l2 )
    {
        ArrayList< tb_res > res = new ArrayList<>();

        int s1 = l1.count();
        int s2 = l2.count();
        
        int i1 = 0;
        int i2 = 0;
        tb_res e1 = l1.get( i1 );
        tb_res e2 = l2.get( i2 );
        while ( e1 != null || e2 != null )
        {
            if ( e1 == null )
            {
                while ( i2 < s2 && res.size() < top_n )
                {
                    res.add( l2.get( i2 ) );
                    i2 += 1;
                }
                e2 = null;
            }
            else if ( e2 == null )
            {
                while ( i1 < s1 && res.size() < top_n )
                {
                    res.add( l1.get( i1 ) );
                    i1 += 1;
                }
                e1 = null;
            }
            else
            {
                if ( res.size() >= top_n )
                {
                    e1 = null;
                    e2 = null;
                }
                else
                {
                    /*
                        0  ... equal
                        -1 ... e1 > e2
                        +1 ... e1 < e2
                    */
                    int cmp = e1.compareTo( e2 );
                    if ( cmp < 0 )
                    {
                        // e1 < e2
                        res.add( e1 );
                        i1 += 1;
                        if ( i1 < s1 )
                            e1 = l1.get( i1 );
                        else
                            e1 = null;
                    }
                    else
                    {
                        // e2 < e1 or e2 == e1
                        res.add( e2 );
                        i2 += 1;
                        if ( i2 < s2 )
                            e2 = l2.get( i2 );
                        else
                            e2 = null;
                    }
                }
            }
        }
		this.results = new tb_res[ res.size() ];
		for ( int i = 0; i < res.size(); ++i )
			this.results[ i ] = res.get( i );
    }

	public int count()
	{
		if ( results != null )
			return results.length;
		return 0;
	}

}

