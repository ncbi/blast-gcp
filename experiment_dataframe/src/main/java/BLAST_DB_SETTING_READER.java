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

public class BLAST_DB_SETTING_READER
{
    public static final String key_selector = "selector";
    public static final String key_location = "location";
    public static final String key_pattern = "pattern";
    public static final String key_bucket = "bucket";
    public static final String key_flat_layout = "flat_layout";
    public static final String key_num_partitions = "num_partitions";
    public static final String key_extensions = "extensions";
    public static final String key_num_locations = "num_locations";

    public static final String  dflt_selector = "nt";
    public static final String  dflt_location = "/tmp/blast/db";
    public static final String  dflt_pattern = "nt_50M";
    public static final String  dflt_bucket = ""; // "nt_50mb_chunks";
    public static final Boolean dflt_flat_layout = false;
    public static final Integer dflt_num_partitions = 0;
    public static final Integer dflt_num_locations = 1;

    public static void defaults( BLAST_DB_SETTING setting )
    {
        setting.selector = dflt_selector;
        setting.location = dflt_location;
        setting.pattern  = dflt_pattern;
        setting.bucket   = dflt_bucket;
        setting.flat_layout     = dflt_flat_layout;
        setting.num_partitions  = dflt_num_partitions;
        setting.num_locations   = dflt_num_locations;
    }

    public static void from_json( JsonObject obj, BLAST_DB_SETTING setting )
    {
        if ( obj != null )
        {
            setting.selector = SE_UTILS.get_json_string( obj, key_selector, dflt_selector );
            setting.location = SE_UTILS.get_json_string( obj, key_location, dflt_location );
            setting.pattern  = SE_UTILS.get_json_string( obj, key_pattern, dflt_pattern );
            setting.bucket   = SE_UTILS.get_json_string( obj, key_bucket, dflt_bucket );
            setting.flat_layout    = SE_UTILS.get_json_bool( obj, key_flat_layout, dflt_flat_layout );
            setting.num_partitions = SE_UTILS.get_json_int( obj, key_num_partitions, dflt_num_partitions );
            SE_UTILS.get_string_list( obj, key_extensions, null, setting.extensions );
            setting.num_locations  = SE_UTILS.get_json_int( obj, key_num_locations, dflt_num_locations );
        }
    }

}
