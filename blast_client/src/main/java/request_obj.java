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

import java.io.StringWriter;
import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;

class request_obj
{
	public String RID;
	public String db_location;
    public String blast_params;
	public String program;
	public String query_seq;
	public String top_N_prelim;
	public String top_N_traceback;

	private String escape_quotes( final String src )
	{
		StringWriter sw = new StringWriter();
		for ( char c: src.toCharArray() )
		{
			if ( c == '"' )
				sw.write( '\\' );
			sw.write( c );
		}
		return sw.toString();
	}

    public request_obj( final String src )
    {
        JsonParser parser = new JsonParser();
        try
        {
			JsonElement tree = parser.parse( src.trim() );
            JsonObject root  = tree.getAsJsonObject();

	        this.RID          = json_utils.get_json_string( root, "RID", "" );
            this.blast_params = escape_quotes( json_utils.get_sub_as_string( root, "blast_params" ) );	// get the orig. text!
    		this.program      = json_utils.get_json_string( root, "program", "" );
			this.query_seq    = json_utils.get_sub_as_string( root, "query_seq" ); // get the orig. text!
    		this.top_N_prelim = json_utils.get_json_string( root, "top_N_prelim", "" );
    		this.top_N_traceback = json_utils.get_json_string( root, "top_N_traceback", "" );
		}
        catch( Exception e )
        {
            System.out.println( String.format( "request_obj : %s", e ) );
        }
    }

	public String toJson( final String db_location )
	{
		this.db_location  = db_location; // insert !!

		// we cannot use the 'classic way' of Gson.toJson( obj ), because downstream parser does not understand proper escaping!
		StringWriter sw = new StringWriter();
		sw.write( "{\n" );
		sw.write( String.format( "\t\"RID\": \"%s\",\n", RID ) );
		sw.write( String.format( "\t\"db_location\": \"%s\",\n", db_location ) );
		sw.write( String.format( "\t\"blast_params\": \"%s\",\n", blast_params ) ); // quotation already in self.blast_params !!!
		sw.write( String.format( "\t\"program\": \"%s\",\n", program ) );
		sw.write( String.format( "\t\"query_seq\": %s,\n", query_seq ) ); // quotation already in self.query_seq !!!
		sw.write( String.format( "\t\"top_N_prelim\": %s,\n", top_N_prelim ) );
		sw.write( String.format( "\t\"top_N_traceback\": %s\n", top_N_traceback ) );
		sw.write( "}\n" );
		return sw.toString();
	}
}
