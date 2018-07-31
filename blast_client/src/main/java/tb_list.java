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

import java.nio.ByteBuffer;
import java.io.FileOutputStream;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;

class tb_list
{
	public String protocol;
	public tb_res[] results;

    public tb_list( final String src )
    {
        JsonParser parser = new JsonParser();
        try
        {
			JsonElement tree = parser.parse( src.trim() );
            JsonObject root  = tree.getAsJsonObject();

	        this.protocol = json_utils.get_json_string( root, "protocol", "" );
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

	public void write_to_file( final String filePath )
	{
		try
		{
		    byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
		    byte[] seq_annot_suffix = { 0, 0, 0, 0, 0, 0, 0, 0 };
			int sum = seq_annot_prefix.length + seq_annot_suffix.length;
			if ( this.results != null )
			{
				for ( int i = 0; i < this.results.length; ++i )
					sum += this.results[ i ].length();
			}

			ByteBuffer buf = ByteBuffer.allocate( sum );
		    buf.put( seq_annot_prefix );
			if ( this.results != null )
			{
		    	for ( int i = 0; i < this.results.length; ++i )
					this.results[ i ].put_to_ByteBuffer( buf );
			}
		    buf.put( seq_annot_suffix );

			FileOutputStream os = new FileOutputStream( filePath );
			os.write( buf.array() );
			os.close();
		}
        catch( Exception e )
        {
            System.out.println( String.format( "tb_list.write_to_file : %s", e ) );
        }

	}

}
