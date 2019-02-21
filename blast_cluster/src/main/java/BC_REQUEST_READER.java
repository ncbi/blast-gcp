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

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.List;
import java.util.ArrayList;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.api.services.storage.Storage;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

class BC_REQUEST_READER
{
	// helper: parse a string into a json-tree
	private static JsonElement parse_string_2_tree( final String line )
	{
		JsonElement res = null;
        try
        {
            JsonParser parser = new JsonParser();
            res = parser.parse( line.trim() );
        }
        catch( Exception e )
        {
            System.out.println( String.format( "BC_REQUEST_READER.parse_string_2_tree() : %s", e ) );
        }
		return res;
	}

	// helper: parse the file ( pointed to by path ) into a json-tree
	private static JsonElement parse_file_2_tree( final String path )
	{
		JsonElement res = null;
        try
        {
            JsonParser parser = new JsonParser();
            res = parser.parse( new FileReader( path ) );
        }
        catch( Exception e )
        {
            System.out.println( String.format( "BC_REQUEST_READER.parse_file_2_tree() : %s", e ) );
        }
		return res;
	}

	// helper: parse the gs_file ( pointed to by uri ) into a json-tree
	private static JsonElement parse_gs_file_2_tree( final URI uri )
	{
		JsonElement res = null;
		try
		{
		    String bucket = uri.getAuthority();
		    String key = uri.getPath();
		    if ( key.startsWith( "/" ) )
		        key = key.substring( 1 );
		    InputStream is = BC_GCP_TOOLS.download_as_stream( bucket, key );
		    if ( is != null )
			{
				JsonParser parser = new JsonParser();
		        res = parser.parse( new InputStreamReader( is ) );
			}
		}
        catch( Exception e )
        {
            System.out.println( String.format( "BC_REQUEST_READER.parse_gs_file_2_tree() : %s", e ) );
        }
		return res;
	}

	// helper: parse file pointed to by path ( can be file or gs-uri ) into a json-tree
	private static JsonElement parse_path_2_tree( final String path )
	{
		JsonElement res = null;
        try
        {
            URI uri = new URI( path );
            if ( uri.getScheme().equals( "gs" ) )
				res = parse_gs_file_2_tree( uri );
			else
				res = parse_file_2_tree( path );
		}
        catch( Exception e )
        {
            res = parse_file_2_tree( path );
        }
		return res;
	}

	// helper: transform a json-tree into a BC_REQUEST-obj / according to protocol '1.0'
    private static BC_REQUEST req_protocol_1_0( final JsonObject root )
    {
		BC_REQUEST res = new BC_REQUEST();
		if ( res != null )
		{
        	res.id = BC_JSON_UTILS.get_json_string( root, "RID", "" );
            res.db = BC_JSON_UTILS.get_json_string( root, "db_tag", "" );
            res.program = BC_JSON_UTILS.get_json_string( root, "program", "" );
            res.top_n_prelim = BC_JSON_UTILS.get_json_int( root, "top_N_prelim", 0 );
            res.top_n_traceback = BC_JSON_UTILS.get_json_int( root, "top_N_traceback", 0 );
            res.query_seq = BC_JSON_UTILS.get_json_string ( root, "query_seq", "" );
            res.params = BC_JSON_UTILS.get_sub_as_string( root, "blast_params" ); // blast_params are allowed to be empty...
		}
        return res;
    }

	// helper: transform a json-tree into a BC_REQUEST-obj / fork by protocol-version
    private static BC_REQUEST parse_tree( final JsonElement tree )
    {
		BC_REQUEST request = null;
        if ( tree.isJsonObject() )
        {
            JsonObject root = tree.getAsJsonObject();
            String protocol = BC_JSON_UTILS.get_json_string ( root, "protocol", "" );
            if ( protocol.equals( "1.0" ) )
                request = req_protocol_1_0( root );
			else
				System.out.println( "BC_REQUEST_READER.parse_tree() : invalid protocol" );			
        }
        return request;
    }

	// used to get request-entry from socket / cmd-line
    public static BC_REQUEST parse_from_string( final String line )
    {
		BC_REQUEST request = null;
		JsonElement tree = parse_string_2_tree( line );
		if ( tree != null )
		{
    		request = parse_tree( tree );
			if ( request != null )
			{
				if ( !request.valid() )
					request = null;
			}
		}
		return request;
    }

	// used to get request-entry from pubsub as string
    public static BC_REQUEST parse_from_string_and_ack( final String line, final String ack )
    {
		BC_REQUEST request = null;
		JsonElement tree = parse_string_2_tree( line );
		if ( tree != null )
		{
    		request = parse_tree( tree );
			if ( request != null )
			{
				if ( !request.valid() )
					request = null;
			}
		}
		return request;
    }

	// used to get the request-entry from a file ( local or in a bucket )
    public static BC_REQUEST parse_from_file( final String path )
    {
		BC_REQUEST request = null;
		JsonElement tree = parse_path_2_tree( path );
		if ( tree != null )
		{
    		request = parse_tree( tree );
			if ( request != null )
			{
				if ( !request.valid() )
					request = null;
			}
		}
		return request;
    }
}

