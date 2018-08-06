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

package gov.nih.nlm.ncbi.blast_client;

import static java.lang.Math.*;
import java.nio.ByteBuffer;
import java.io.Serializable;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;

class tb_res implements Serializable, Comparable< tb_res >
{
	private byte blob[];
	public int oid;
	public int[] ties;

    public tb_res( JsonElement je )
	{
		try
		{
			JsonObject jo = je.getAsJsonObject();
			if ( jo != null )
			{
				populate_blob( jo );
				oid = json_utils.get_json_int( jo, "oid", 0 );
				populate_ties( jo );
			}
		}
        catch( Exception e )
        {
			System.out.println( String.format( "tb_res : %s", e ) );
        }
	}

	public int length()
	{
		if ( this.blob != null )
			return blob.length;
		return 0;
	}

	public void put_to_ByteBuffer( ByteBuffer buf )
	{
		if ( this.blob != null )
			buf.put( this.blob );
	}

	private void populate_blob( JsonObject jo )
	{
		try
		{
			String hex = json_utils.get_json_string( jo, "asn1_blob", "" );
			this.blob = json_utils.hexStringToByteArray( hex );
		}
        catch( Exception e )
        {
			System.out.println( String.format( "tb_res.populate_blob : %s", e ) );
        }
	}

	private void populate_ties( JsonObject jo )
	{
		try
		{
			JsonArray a = jo.getAsJsonArray( "ties" );
			int n = a.size();
			this.ties = new int[ n ];
			for ( int i = 0; i < n; i++ )
			{
				JsonElement e = a.get( i );
				this.ties[ i ] = e.getAsInt();
			}
		}
        catch( Exception e )
        {
			System.out.println( String.format( "tb_res.populate_ties : %s", e ) );
        }
	}

    /*
        0  ... equal
        -1 ... this > other
        +1 ... this < other
    */
    @Override public int compareTo( tb_res other )
    {
		if ( other == null )
			return 0;
		if ( this.ties == null || other.ties == null )
			return 0;

		int l = min( this.ties.length, other.ties.length );
		if ( l < 1 )
			return 0;

		for ( int i = 0; i < l; ++i )
		{
        	if ( this.ties[ i ] > other.ties[ i ] )
				return -1;
        	if ( this.ties[ i ] < other.ties[ i ] )
				return 1;
			// what is left now is : this.ties[ i ] < other.ties[ i ]
			// in this case we continue the loop, to compare the next tie.
        }
		// what is left now is : all ties are the same
		// now we have to return 0 ( aka equal )
		return 0;
    }

}
