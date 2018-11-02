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

import com.google.gson.JsonObject;
import java.util.List;
import java.util.ArrayList;

public class BLAST_DB_SETTING_READER
{
    public static final String key_selector = "selector";
    public static final String key_pattern = "pattern";
    public static final String key_bucket = "bucket";
    public static final String key_flat_layout = "flat_layout";
    public static final String key_extensions = "extensions";

    public static final String  dflt_selector = "nt";
    public static final String  dflt_pattern = "nt_50M";
    public static final String  dflt_bucket = ""; // "nt_50mb_chunks";
    public static final Boolean dflt_flat_layout = false;

	/* synthesizing the names, extensions from the settings-json-file */
    public static BLAST_DB_SETTING from_json( JsonObject obj,
											  Integer volume_limit,
											  final String loc,
											  Integer num_locations )
    {
        BLAST_DB_SETTING res = new BLAST_DB_SETTING();
        if ( obj != null )
        {
            res.key         = SE_UTILS.get_json_string( obj, key_selector, dflt_selector );    /* key from request... */
            res.location   = loc;    /* where is the location root on the worker? */
            String pattern = SE_UTILS.get_json_string( obj, key_pattern, dflt_pattern );
            res.bucket     = SE_UTILS.get_json_string( obj, key_bucket, dflt_bucket );
            res.flat_layout    = SE_UTILS.get_json_bool( obj, key_flat_layout, dflt_flat_layout );
            List< String > extensions = new ArrayList<>();
            SE_UTILS.get_string_list( obj, key_extensions, null, extensions );
            res.num_locations  = num_locations;
            res.volume_count   = 0;
            res.seq_count      = 0L;
            res.seq_lenght     = 0L;

            /* now let as create the volumes... */
            for ( int i = 0; i < volume_limit; ++i )
            {
                String name;
                if ( i < 100 )
                    name = String.format( "%s.%02d", pattern, i  );
                else
                    name = String.format( "%s.%d", pattern, i );
        
                CONF_VOLUME vol = new CONF_VOLUME( name, res.key, res.bucket );
                for ( String extension : extensions )
                {
					String ext_name = String.format( "%s.%s", name, extension );
                    CONF_VOLUME_FILE vol_f = new CONF_VOLUME_FILE( 
                    		ext_name,
                    		"",   // we do not know without manifest-file
                    		String.format( "%s/%s/%s", loc, name, ext_name ) );
                    vol.files.add( vol_f );
                }
                res.volumes.add( vol );
            }
        }
        return res;
    }

	/* using the manifest-file as a source for the BLAST-DB: extensions, names,
	   inserting the location */
    public static BLAST_DB_SETTING from_conf( CONF_DATABASE conf,
											  Integer volume_limit,
											  final String loc,
											  Integer num_locations )
    {
        BLAST_DB_SETTING res = new BLAST_DB_SETTING();
        if ( conf != null )
        {
            res.key      = conf.db_tag;     /* the tag will be the key the request joins with the database */
            res.location = loc;
            res.bucket   = conf.bucket;
            res.flat_layout   = false; /* we do not use a flat layout in case of manifest */
            res.num_locations = num_locations;
            res.volume_count  = conf.volume_count;
            res.seq_count     = conf.seq_count;
            res.seq_lenght    = conf.seq_length;

            /* now let as assign the volumes from the manifest ... */
            int n = conf.volumes.size();
            if ( volume_limit > 0 && n > volume_limit )
                n = volume_limit;
            for ( CONF_VOLUME v : conf.volumes.subList( 0, n ) )
            {
                CONF_VOLUME vol = new CONF_VOLUME( v.name, res.key, res.bucket );
                for ( CONF_VOLUME_FILE f : v.files )
                {
                    vol.files.add( new CONF_VOLUME_FILE( f, loc, v.name ) );
                }
                res.volumes.add( vol );
            }
        }
        return res;
    }
}
