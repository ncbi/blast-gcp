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

    public static BLAST_REQUEST parse( final String line, final Integer top_n )
    {
        String S = line.trim();
        if ( S.startsWith( "{" ) )
            return parse_json( S, top_n );
        return parse_colons_sep_list( S, top_n );
    }

    private static BLAST_REQUEST parse_colons_sep_list( final String line, final Integer top_n )
    {
        BLAST_REQUEST res = new BLAST_REQUEST();

        String[] parts  = line.split( "\\:" );
        res.id         = ( parts.length > 0 ) ? parts[ 0 ] : dflt_id;
        res.query      = ( parts.length > 1 ) ? parts[ 1 ] : dflt_query;
        res.top_n      = ( parts.length > 2 ) ? Integer.parseInt( parts[ 2 ] ) : top_n;
        res.db         = ( parts.length > 3 ) ? parts[ 3 ] : dflt_db;
        res.program    = ( parts.length > 4 ) ? parts[ 4 ] : dflt_program;
        res.params     = ( parts.length > 5 ) ? parts[ 5 ] : dflt_params;

        return res;
    }

    private static void set_defaults( BLAST_REQUEST req, final Integer top_n )
    {
        if ( req.id.isEmpty() ) req.id = dflt_id;
        if ( req.query.isEmpty() ) req.query = dflt_query;
        if ( req.top_n < 1 ) req.top_n = top_n;
        if ( req.db.isEmpty() ) req.db = dflt_db;
        if ( req.program.isEmpty() ) req.program = dflt_program;
        if ( req.params.isEmpty() ) req.params = dflt_params;
    }

    private static String get_json_string( JsonObject root, final String name, final String dflt )
    {
        String res = dflt;
        JsonElement elem = root.get( name );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsString();
            }
            catch( Exception e )
            {
            }
        }
        return res;
    }

    private static Integer get_json_int( JsonObject root, final String name, final Integer dflt )
    {
        Integer res = dflt;
        JsonElement elem = root.get( name );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsInt();
            }
            catch( Exception e )
            {
            }
        }
        return res;
    }

    private static BLAST_REQUEST parse_json( final String line, final Integer top_n )
    {
        BLAST_REQUEST res = new BLAST_REQUEST();
        JsonParser parser = new JsonParser();
        try
        {
            JsonElement tree = parser.parse( line.trim() );
            if ( tree.isJsonObject() )
            {
                JsonObject root = tree.getAsJsonObject();

                res.id = get_json_string( root, "RID", dflt_id );
                JsonElement blast_params_elem = root.get( "blast_params" );
                if ( blast_params_elem != null )
                {
                    if ( blast_params_elem.isJsonObject() )
                    {
                        JsonObject blast_params = blast_params_elem.getAsJsonObject();
                        if ( blast_params != null )
                        {
                            if ( blast_params.isJsonObject() )
                            {
                                res.db = get_json_string( blast_params, "db", dflt_db );
                                res.program = get_json_string( blast_params, "program", dflt_program );
                                res.params = res.program;
                                res.top_n = get_json_int( blast_params, "hitlist_size", top_n );

                                try
                                {
                                    JsonElement queries = blast_params.get( "queries" );
                                    if ( queries != null )
                                    {
                                        JsonArray query_array = queries.getAsJsonArray();
                                        if ( query_array.size() > 0 )
                                        {
                                            JsonElement first_query = query_array.get( 0 );
                                            res.query = first_query.getAsString();
                                        }
                                    }
                                }
                                catch( Exception e )
                                {
                                }
                            }
                        }
                    }
                }
            }
        }
        catch( Exception e )
        {
            set_defaults( res, top_n );            
        }
        return res;
    }
}

