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
    // ------------------- default values -----------------------------------------
    public static final String dflt_id = "Req_Id_0001";
    public static final String dflt_query = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
    public static final String dflt_db = "nt";
    public static final String dflt_program = "blastn";
    public static final String dflt_params = "blastn";

    private static BLAST_REQUEST parse( final String line, final Integer top_n )
    {
        BLAST_REQUEST res = null;
        String S = line.trim();
        if ( S.startsWith( "{" ) )
            res = parse_json( S, top_n );
        else
            res = parse_colons_sep_list( S, top_n );
        return res;
    }

    private static BLAST_REQUEST parse_colons_sep_list( final String line, final Integer top_n )
    {
        BLAST_REQUEST res = new BLAST_REQUEST();

        String[] parts  = line.split( "\\:" );
        res.id         = ( parts.length > 0 ) ? parts[ 0 ] : dflt_id;
        res.query_seq  = ( parts.length > 1 ) ? parts[ 1 ] : dflt_query;
        res.top_n_prelim = ( parts.length > 2 ) ? Integer.parseInt( parts[ 2 ] ) : top_n;
        res.top_n_traceback = res.top_n_prelim;
        res.db         = ( parts.length > 3 ) ? parts[ 3 ] : dflt_db;
        res.program    = ( parts.length > 4 ) ? parts[ 4 ] : dflt_program;
        res.params     = ( parts.length > 5 ) ? parts[ 5 ] : dflt_params;

        return res;
    }

    private static String append_string_param( final String src, JsonObject root, final String key, final String dflt, final String term )
    {
        return src + String.format( "\"%s\": \"%s\"%s ", key, SE_UTILS.get_json_string( root, key, dflt ), term );
    }

    private static String append_int_param( final String src, JsonObject root, final String key, int dflt, final String term )
    {
        return src + String.format( "\"%s\": %d%s ", key, SE_UTILS.get_json_int( root, key, dflt ), term );
    }

    private static String append_string_to_long_param( final String src, JsonObject root, final String key, Long dflt, final String term )
    {
        Long l_value = dflt;
        String s_value = SE_UTILS.get_json_string( root, key, "" );
        if ( !s_value.isEmpty() )
            l_value = Long.parseLong( s_value, 10 );
        return src + String.format( "\"%s\": %d%s ", key, l_value, term );
    }

    private static String append_double_param( final String src, JsonObject root, final String key, double dflt, final String term )
    {
        return src + String.format( "\"%s\": %.1f%s ", key, SE_UTILS.get_json_double( root, key, dflt ), term );
    }

    private static String append_bool_param( final String src, JsonObject root, final String key, boolean dflt, final String term )
    {
        if ( SE_UTILS.get_json_bool( root, key, dflt ) )
            return src + String.format( "\"%s\": true%s ", key, term );
        else
            return src + String.format( "\"%s\": false%s ", key, term );
    }

    private static String extract_params( BLAST_REQUEST request, JsonObject root )
    {
        String res = String.format( "{\"db\":\"%s\", ", request.db );
        res = append_string_to_long_param( res, root, "db_length", 0L, "," );
        res = append_string_to_long_param( res, root, "db_num_seqs", 0L, "," );
        res = append_double_param( res, root, "evalue", 10.0, "," );
        res = append_string_param( res, root, "filter_string", "F", "," );
        //res = append_int_param( res, root, "gap_extend", 1, "," );
        //res = append_int_param( res, root, "gap_open", 11, "," );
        //res = append_bool_param( res, root, "gapped_alignment", false, "," );
        res = append_int_param( res, root, "hitlist_size", 100, "," );
        res = append_string_param( res, root, "matrix", "BLOSUM62", "," );
        res = append_double_param( res, root, "perc_identity", 0.0, "," );
        res = res + String.format( "\"program\":\"%s\", ", request.program );
        res = append_int_param( res, root, "window_size", 40, "," );
        res = append_int_param( res, root, "word_size", 6 , ",");
        res = append_int_param( res, root, "word_threshold", 21, "}" );
        return res;
    }

    private static boolean req_protocol_1_0( BLAST_REQUEST request, JsonObject root )
    {
        request.id = SE_UTILS.get_json_string( root, "RID", "" );
        boolean res = !request.id.isEmpty();
        if ( res )
        {
            request.db = SE_UTILS.get_json_string( root, "db_tag", "" );
            res = !request.db.isEmpty();
        }
        if ( res )
        {
            request.program = SE_UTILS.get_json_string( root, "program", "" );
            res = !request.program.isEmpty();
        }
        if ( res )
        {
            request.top_n_prelim = SE_UTILS.get_json_int( root, "top_N_prelim", 0 );
            res = ( request.top_n_prelim > 0 );
        }
        if ( res )
        {
            request.top_n_traceback = SE_UTILS.get_json_int( root, "top_N_traceback", 0 );
            res = ( request.top_n_traceback > 0 );
        }
        if ( res )
        {
            request.query_seq = SE_UTILS.get_json_string ( root, "query_seq", "" );
            res = !request.query_seq.isEmpty();
        }
        if ( res )
            request.params = SE_UTILS.get_sub_as_string( root, "blast_params" );
            /* blast_params are allowed to be empty... */
        return res;
    }

    private static boolean parse_json_tree( BLAST_REQUEST request, JsonElement tree, final Integer top_n )
    {
        boolean res = false;
        if ( tree.isJsonObject() )
        {
            JsonObject root = tree.getAsJsonObject();
            String protocol = SE_UTILS.get_json_string ( root, "protocol", "" );
            if ( protocol.equals( "1.0" ) )
                res = req_protocol_1_0( request, root );
            else
              System.err.println( "Invalid BLAST request - no protocol field" );
        }
        return res;
    }

    private static BLAST_REQUEST parse_json( final String line, final Integer top_n )
    {
        BLAST_REQUEST request = new BLAST_REQUEST();
        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( line.trim() );
            if ( !parse_json_tree( request, tree, top_n ) )
                request = null;
        }
        catch( Exception e )
        {
            request = null;
        }
        return request;
    }

    public static REQUESTQ_ENTRY parse_from_string( final String line, final Integer top_n, final Boolean skip_jni )
    {
        BLAST_REQUEST request = parse( line, top_n );
        if ( request != null )
        {
            request.skip_jni = skip_jni;
            return new REQUESTQ_ENTRY( request );
        }
        return null;
    }

    public static REQUESTQ_ENTRY parse_from_string_and_ack( final String line, final String ack, final Integer top_n, final Boolean skip_jni )
    {
        BLAST_REQUEST request = parse( line, top_n );
        if ( request != null )
        {
            request.skip_jni = skip_jni;
            return new REQUESTQ_ENTRY( request, ack );
        }
        return null;
    }

    public static REQUESTQ_ENTRY parse_from_file( final String filename, final Integer top_n, final Boolean skip_jni )
    {
        JsonParser parser = new JsonParser();
        try
        {
            JsonElement tree = null;

            try
            {
                URI uri = new URI( filename );
                if ( uri.getScheme().equals( "gs" ) )
                {
                    Storage storage = BLAST_GS_DOWNLOADER.buildStorageService();
                    String bucket = uri.getAuthority();
                    String key = uri.getPath();
                    if ( key.startsWith( "/" ) )
                        key = key.substring( 1 );
                    InputStream is = BLAST_GS_DOWNLOADER.download_as_stream( storage, bucket, key );
                    if ( is != null )
                        tree = parser.parse( new InputStreamReader( is ) );
                    else
                        System.out.println( String.format( "no inputstream from: '%s'", filename ) );                    
                }
                else
                {
                    tree = parser.parse( new FileReader( filename ) );
                }
            }
            catch( Exception e )
            {
                tree = parser.parse( new FileReader( filename ) );
            }

            if ( tree != null )
            {
                BLAST_REQUEST request = new BLAST_REQUEST();
                parse_json_tree( request, tree, top_n );
                request.skip_jni = skip_jni;
                return new REQUESTQ_ENTRY( request );
            }
        }
        catch( Exception e )
        {
            System.out.println( String.format( "error parsing from file: '%s'", filename ) );
        }
        return null;
    }

}
