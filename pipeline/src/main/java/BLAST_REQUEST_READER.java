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

class BLAST_REQUEST_READER
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
            System.out.println( String.format( "BLAST_REQUEST_READER.parse_string_2_tree() : %s", e ) );
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
            System.out.println( String.format( "BLAST_REQUEST_READER.parse_file_2_tree() : %s", e ) );
        }
		return res;
	}

	// helper: parse the gs_file ( pointed to by uri ) into a json-tree
	private static JsonElement parse_gs_file_2_tree( final URI uri )
	{
		JsonElement res = null;
		try
		{
		    Storage storage = BLAST_GS_DOWNLOADER.buildStorageService();
		    String bucket = uri.getAuthority();
		    String key = uri.getPath();
		    if ( key.startsWith( "/" ) )
		        key = key.substring( 1 );
		    InputStream is = BLAST_GS_DOWNLOADER.download_as_stream( storage, bucket, key );
		    if ( is != null )
			{
				JsonParser parser = new JsonParser();
		        res = parser.parse( new InputStreamReader( is ) );
			}
		}
        catch( Exception e )
        {
            System.out.println( String.format( "BLAST_REQUEST_READER.parse_gs_file_2_tree() : %s", e ) );
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

	// helper: transform a json-tree into a BLAST_REQUEST-obj / according to protocol '1.0'
    private static BLAST_REQUEST req_protocol_1_0( final JsonObject root )
    {
		BLAST_REQUEST res = new BLAST_REQUEST();
		if ( res != null )
		{
        	res.id = SE_UTILS.get_json_string( root, "RID", "" );
            res.db = SE_UTILS.get_json_string( root, "db_tag", "" );
            res.program = SE_UTILS.get_json_string( root, "program", "" );
            res.top_n_prelim = SE_UTILS.get_json_int( root, "top_N_prelim", 0 );
            res.top_n_traceback = SE_UTILS.get_json_int( root, "top_N_traceback", 0 );
            res.query_seq = SE_UTILS.get_json_string ( root, "query_seq", "" );
            res.params = SE_UTILS.get_sub_as_string( root, "blast_params" ); // blast_params are allowed to be empty...
		}
        return res;
    }

	// helper: transform a json-tree into a BLAST_REQUEST-obj / fork by protocol-version
    private static BLAST_REQUEST parse_tree( final JsonElement tree )
    {
		BLAST_REQUEST res = null;
        if ( tree.isJsonObject() )
        {
            JsonObject root = tree.getAsJsonObject();
            String protocol = SE_UTILS.get_json_string ( root, "protocol", "" );
            if ( protocol.equals( "1.0" ) )
                res = req_protocol_1_0( root );
        }
        return res;
    }

	// used to get request-entry from socket / cmd-line
    public static REQUESTQ_ENTRY parse_from_string( final String line )
    {
		REQUESTQ_ENTRY res = null;
		JsonElement tree = parse_string_2_tree( line );
		if ( tree != null )
		{
    		BLAST_REQUEST req = parse_tree( tree );
			if ( req != null )
			{
				if ( req.valid() )
					res = new REQUESTQ_ENTRY( req );
				else
					System.out.println( "BLAST_REQUEST_READER.parse_from_string() : invalid request" );
			}
		}
		return res;
    }

	// used to get request-entry from pubsub as string
    public static REQUESTQ_ENTRY parse_from_string_and_ack( final String line, final String ack )
    {
		REQUESTQ_ENTRY res = null;
		JsonElement tree = parse_string_2_tree( line );
		if ( tree != null )
		{
    		BLAST_REQUEST req = parse_tree( tree );
			if ( req != null )
			{
				if ( req.valid() )
					res = new REQUESTQ_ENTRY( req, ack );
				else
					System.out.println( "BLAST_REQUEST_READER.parse_from_string_and_ack() : invalid request" );
			}
		}
		return res;
    }

	// used to get the request-entry from a file ( local or in a bucket )
    public static REQUESTQ_ENTRY parse_from_file( final String path )
    {
		REQUESTQ_ENTRY res = null;
		JsonElement tree = parse_path_2_tree( path );
		if ( tree != null )
		{
    		BLAST_REQUEST req = parse_tree( tree );
			if ( req != null )
			{
				if ( req.valid() )
					res = new REQUESTQ_ENTRY( req );
				else
					System.out.println( "BLAST_REQUEST_READER.parse_from_file() : invalid request" );
			}
		}
		return res;
    }
}

