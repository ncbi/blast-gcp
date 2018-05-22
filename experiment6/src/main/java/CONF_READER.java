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

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.stream.JsonReader;

import com.google.api.services.storage.Storage;

import java.net.URI;
import java.net.URISyntaxException;

public class CONF_READER
{
    public static final String key_db_type = "type";
    public static final String key_db_manifest_url = "manifest";
    public static final String key_db_tag = "db_tag";
    public static final String key_volume_count = "volume_count";
    public static final String key_seq_count = "sequence_count";
    public static final String key_seq_length = "sequence_length";
    public static final String key_extensions = "pipeline_extensions";
    public static final String key_updated = "update_date";
    public static final String key_bucket = "gs_bucket_name";
    public static final String key_volumes = "volumes";
    public static final String key_vol_name = "volume";
    public static final String key_vol_files = "files";
    public static final String key_vol_file_type = "type";
    public static final String key_vol_file_name = "name";
    public static final String key_vol_file_md5 = "md5_hash:";
    
    public static CONF_DATABASE read_conf_database( JsonObject obj )
    {
        CONF_DATABASE res = new CONF_DATABASE();
        if ( obj != null )
        {
            res.db_type          = SE_UTILS.get_json_string( obj, key_db_type, "" );
            res.db_manifest_url  = SE_UTILS.get_json_string( obj, key_db_manifest_url, "" );
        }
        return res;
    }

    private static void read_db_manifest_volumes_files( CONF_VOLUME vol, JsonArray file_list )
    {
        if ( file_list != null )
        {
            for ( JsonElement e : file_list ) 
            {
                if ( e.isJsonObject() )
                {
                    JsonObject file_obj = e.getAsJsonObject();
                    CONF_VOLUME_FILE f = new CONF_VOLUME_FILE();
                    f.f_type = SE_UTILS.get_json_string( file_obj, key_vol_file_type, "" );
                    f.f_name = SE_UTILS.get_json_string( file_obj, key_vol_file_name, "" );
                    f.f_md5 = SE_UTILS.get_json_string( file_obj, key_vol_file_md5, "" );
                    vol.files.add( f );
                }
            }
        }
    }

    private static void read_db_manifest_volumes( CONF_DATABASE db, JsonArray vol_list )
    {
        if ( vol_list != null )
        {
            for ( JsonElement e : vol_list ) 
            {
                if ( e.isJsonObject() )
                {
                    JsonObject vol_obj = e.getAsJsonObject();
                    CONF_VOLUME vol = new CONF_VOLUME( SE_UTILS.get_json_string( vol_obj, key_vol_name, "" ) );
                    read_db_manifest_volumes_files( vol, SE_UTILS.get_sub_array( vol_obj, key_vol_files ) );

                    db.volumes.add( vol );
                }
            }
        }
    }

    private static void read_db_manifest( CONF_DATABASE db, Storage storage, JsonParser parser )
    {
        InputStream manifest_stream = BLAST_GS_DOWNLOADER.download_uri_as_stream( storage, db.db_manifest_url );
        if ( manifest_stream != null )
        {
            try
            {
                JsonElement tree = parser.parse( new InputStreamReader( manifest_stream ) );
                if ( tree.isJsonObject() )
                {
                    JsonObject root = tree.getAsJsonObject();
                    db.db_tag = SE_UTILS.get_json_string( root, key_db_tag, "" );
                    db.volume_count = SE_UTILS.get_json_int( root, key_volume_count, 0 );
                    db.seq_count = SE_UTILS.get_json_long( root, key_seq_count, 0L );
                    db.seq_length = SE_UTILS.get_json_long( root, key_seq_length, 0L );
                    SE_UTILS.get_string_list( root, key_extensions, null, db.extensions );
                    db.updated = SE_UTILS.get_json_string( root, key_updated, "" );
                    db.bucket = SE_UTILS.get_json_string( root, key_bucket, "" );

                    read_db_manifest_volumes( db, SE_UTILS.get_sub_array( root, key_volumes ) );
                }
            }
            catch( Exception e )
            {
                System.out.println( String.format( "json-parsing: %s", e ) );
            }
            
        }
    }

    public static CONF read_conf( String filename )
    {
        CONF res = new CONF();

        JsonParser parser = new JsonParser();
        try
        {
            JsonElement tree = parser.parse( new FileReader( filename ) );
            if ( tree.isJsonObject() )
            {
                JsonArray db_array = SE_UTILS.get_sub_array( tree.getAsJsonObject(), "databases" );
                if ( db_array != null )
                {
                    for ( JsonElement e : db_array ) 
                    {
                        if ( e.isJsonObject() )
                            res.dbs.add( read_conf_database( e.getAsJsonObject() ) );
                    }

                }
            }
        }
        catch( Exception e )
        {
            System.out.println( String.format( "json-parsing: %s", e ) );
        }

        try
        {
            Storage storage = BLAST_GS_DOWNLOADER.buildStorageService();

            for ( CONF_DATABASE e : res.dbs )
                read_db_manifest( e, storage, parser );
        }
        catch( Exception e )
        {
            System.out.println( String.format( "reading manifests: %s", e ) );
        }


        return res; 
    }
}

