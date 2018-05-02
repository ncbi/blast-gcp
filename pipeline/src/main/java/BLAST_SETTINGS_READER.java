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

import java.util.List;
import java.util.ArrayList;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.FileReader;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

public class BLAST_SETTINGS_READER
{
    // ------------------- sections in json-file ----------------------------------
    public static final String key_source_section = "source";
    public static final String key_socket_section = "socket";
    public static final String key_pubsub_section = "pubsub";
    public static final String key_log_section = "log";
    public static final String key_result_section = "result";
    public static final String key_asn1_section = "asn1";
    public static final String key_status_section = "status";
    public static final String key_blastjni_section = "blastjni";
    public static final String key_db_section = "db";
    public static final String key_spark_section = "spark";

    // ------------------- keys in json-file ---------------------------------------
    public static final String key_appName = "appName";
    public static final String key_spark_log_level = "log_level";
    public static final String key_db_location = "db_location";
    public static final String key_db_pattern = "db_pattern";
    public static final String key_db_bucket = "db_bucket";
    public static final String key_batch_duration = "batch_duration";
    public static final String key_locality_wait = "locality_wait";
    public static final String key_with_locality = "with_locality";
    public static final String key_with_dyn_alloc = "with_dyn_alloc";
    public static final String key_log_host = "log_host";
    public static final String key_trigger_host = "trigger_host";
    public static final String key_log_port = "log_port";
    public static final String key_trigger_port = "trigger_port";
    public static final String key_save_dir = "save_dir";
    public static final String key_num_db_partitions = "num_db_partitions";
    public static final String key_rec_max_rate = "receiver_max_rate";
    public static final String key_top_n = "top_n";
    public static final String key_num_executors = "num_executors";
    public static final String key_num_executor_cores = "num_executor_cores";
    public static final String key_executor_memory = "executor_memory";
    public static final String key_flat_db_layout = "flat_db_layout";
    public static final String key_log_request = "log_request";
    public static final String key_log_start = "log_start";
    public static final String key_log_done = "log_done";
    public static final String key_log_cutoff = "log_cutoff";
    public static final String key_log_final = "log_final";
    public static final String key_log_part_prep = "log_partition_prep";
    public static final String key_log_worker_shift = "log_worker_shift";
    public static final String key_log_pref_loc = "log_pref_loc";
    public static final String key_jni_log_level = "jni_log_level";
    public static final String key_project_id = "project_id";
    public static final String key_subscript_id = "subscript_id";
    public static final String key_gs_bucket = "bucket";
    public static final String key_gs_file = "file";
    public static final String key_gs_running = "running";
    public static final String key_gs_done = "done";
    public static final String key_gs_error = "error";

    // ------------------- default values -----------------------------------------
    public static final String  dflt_spark_log_level = "ERROR";
    public static final String  dflt_db_location = "/tmp/blast/db";
    public static final String  dflt_db_pattern = "nt_50M";
    public static final String  dflt_db_bucket = ""; // "nt_50mb_chunks";
    public static final Integer dflt_batch_duration = 2;
    public static final String  dflt_locality_wait = "3s";
    public static final Boolean dflt_with_locality = false;
    public static final Boolean dflt_with_dyn_alloc = false;
    public static final String  dflt_log_host = "";
    public static final Integer dflt_log_port = 0; //10011;
    public static final String  dflt_trigger_host = "";
    public static final Integer dflt_trigger_port = 0; //10012;
    public static final Integer dflt_num_db_partitions = 0;
    public static final Integer dflt_receiver_max_rate = 5;
    public static final Integer dflt_top_n = 10;
    public static final Integer dflt_num_executors = 10;
    public static final Integer dflt_num_executor_cores = 5;
    public static final String  dflt_executor_memory = "";
    public static final Boolean dflt_flat_db_layout = false;
    public static final Boolean dflt_log_request = true;
    public static final Boolean dflt_log_start = false;
    public static final Boolean dflt_log_done = false;
    public static final Boolean dflt_log_cutoff = true;
    public static final Boolean dflt_log_final = true;
    public static final Boolean dflt_log_part_prep = false;
    public static final Boolean dflt_log_worker_shift = false;
    public static final Boolean dflt_log_pref_loc = false;
    public static final String  dflt_jni_log_level = "Info";
    public static final String  dflt_project_id = "";
    public static final String  dflt_subscript_id = "";
    public static final String  dflt_gs_bucket = "";
    public static final String  dflt_gs_result_file = "output/%s/seq-annot.asn";
    public static final String  dflt_gs_status_file = "status/%s/status.txt";
    public static final String  dflt_gs_running = "RUNNING";
    public static final String  dflt_gs_done = "DONE";
    public static final String  dflt_gs_error = "ERROR";

    private static String dflt_save_dir()
    {
        final String username = System.getProperty( "user.name" );
        return String.format( "hdfs:///user/%s/results/", username );
    }

    public static BLAST_SETTINGS defaults( final String appName )
    {
        BLAST_SETTINGS res = new BLAST_SETTINGS();
        res.appName = appName;
        
        res.db_location = dflt_db_location;
        res.db_pattern  = dflt_db_pattern;
        res.db_bucket   = dflt_db_bucket;

        res.batch_duration  = dflt_batch_duration;
        res.locality_wait   = dflt_locality_wait;
        res.with_locality   = dflt_with_locality;
        res.with_dyn_alloc  = dflt_with_dyn_alloc;
        res.spark_log_level = dflt_spark_log_level;

        res.log_host     = dflt_log_host;
        res.log_port = dflt_log_port;

        res.trigger_host = dflt_trigger_host;
        res.trigger_port = dflt_trigger_port;
        
        res.save_dir = dflt_save_dir();
        
        res.num_db_partitions   = dflt_num_db_partitions;
        res.receiver_max_rate   = dflt_receiver_max_rate;
        res.top_n               = dflt_top_n;

        res.num_executors       = dflt_num_executors;
        res.num_executor_cores  = dflt_num_executor_cores;
        res.executor_memory     = dflt_executor_memory;

        res.flat_db_layout      = dflt_flat_db_layout;

        res.log_request         = dflt_log_request;
        res.log_job_start       = dflt_log_start;
        res.log_job_done        = dflt_log_done;
        res.log_cutoff          = dflt_log_cutoff;
        res.log_final           = dflt_log_final;
        res.log_part_prep       = dflt_log_part_prep;
        res.log_worker_shift    = dflt_log_worker_shift;
        res.log_pref_loc        = dflt_log_pref_loc;
        res.jni_log_level       = dflt_jni_log_level;

        res.project_id          = dflt_project_id;
        res.subscript_id        = dflt_subscript_id;

        res.gs_result_bucket    = dflt_gs_bucket;
        res.gs_result_file      = dflt_gs_result_file;

        res.gs_status_bucket    = dflt_gs_bucket;
        res.gs_status_file      = dflt_gs_status_file;
        res.gs_status_running   = dflt_gs_running;
        res.gs_status_done      = dflt_gs_done;
        res.gs_status_error     = dflt_gs_error;

        return res;
    }
    
    private static String get_json_string( JsonObject root, final String key, final String dflt )
    {
        String res = dflt;
        JsonElement elem = root.get( key );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsString();
            }
            catch( Exception e )
            {
                System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
            }
        }
        return res;
    }

    private static Integer get_json_int( JsonObject root, final String key, final Integer dflt )
    {
        Integer res = dflt;
        JsonElement elem = root.get( key );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsInt();
            }
            catch( Exception e )
            {
                System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
            }
        }
        return res;
    }

    private static Boolean get_json_bool( JsonObject root, final String key, final Boolean dflt )
    {
        Boolean res = dflt;
        JsonElement elem = root.get( key );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsBoolean();
            }
            catch( Exception e )
            {
                System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
            }
        }
        return res;
    }

    private static JsonObject get_sub( JsonObject root, final String key )
    {
        JsonElement e = root.get( key );
        if ( e != null )
        {
            if ( e.isJsonObject() )
                return e.getAsJsonObject();
        }
        return null;
    }

    private static String get_host( JsonObject root, final String key, final String dflt )
    {
        String dflt_host = dflt;
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            dflt_host = localMachine.getHostName();
        }
        catch ( UnknownHostException e )
        {
            dflt_host = dflt;
        }
        return get_json_string( root, key, dflt_host );
    }


    private static void read_spark_settings( BLAST_SETTINGS res, JsonObject root )
    {
        JsonObject spark_obj = get_sub( root, key_spark_section );
        if ( spark_obj != null )
        {
            res.batch_duration = get_json_int( spark_obj, key_batch_duration, dflt_batch_duration );
            res.locality_wait  = get_json_string( spark_obj, key_locality_wait, dflt_locality_wait );
            res.with_locality  = get_json_bool( spark_obj, key_with_locality, dflt_with_locality );
            res.with_dyn_alloc = get_json_bool( spark_obj, key_with_dyn_alloc, dflt_with_dyn_alloc );
            res.spark_log_level = get_json_string( spark_obj, key_spark_log_level, dflt_spark_log_level );
            res.num_executors       = get_json_int( spark_obj, key_num_executors, dflt_num_executors );
            res.num_executor_cores  = get_json_int( spark_obj, key_num_executor_cores, dflt_num_executor_cores );
            res.executor_memory     = get_json_string( spark_obj, key_executor_memory, dflt_executor_memory );
        }
    }

    private static void read_blastjni_settings( BLAST_SETTINGS res, JsonObject root )
    {
        JsonObject blastjni_obj = get_sub( root, key_blastjni_section );
        if ( blastjni_obj != null )
        {
            JsonObject db_obj = get_sub( blastjni_obj, key_db_section );
            if ( db_obj != null )
            {
                res.db_location = get_json_string( db_obj, key_db_location, dflt_db_location );
                res.db_pattern  = get_json_string( db_obj, key_db_pattern, dflt_db_pattern );
                res.db_bucket   = get_json_string( db_obj, key_db_bucket, dflt_db_bucket );
                res.flat_db_layout    = get_json_bool( db_obj, key_flat_db_layout, dflt_flat_db_layout );
                res.num_db_partitions = get_json_int( db_obj, key_num_db_partitions, dflt_num_db_partitions );

            }
            res.top_n         = get_json_int( blastjni_obj, key_top_n, dflt_top_n );
            res.jni_log_level = get_json_string( blastjni_obj, key_jni_log_level, dflt_jni_log_level );
        }
    }

    private static void read_source_settings( BLAST_SETTINGS res, JsonObject root )
    {
        JsonObject source_obj = get_sub( root, key_source_section );
        if ( source_obj != null )
        {
            JsonObject socket_obj = get_sub( source_obj, key_socket_section );
            if ( socket_obj != null )
            {
                res.trigger_port = get_json_int( socket_obj, key_trigger_port, dflt_trigger_port );
                res.trigger_host = get_host( socket_obj, key_trigger_host, dflt_trigger_host );
            }

            JsonObject pubsub_obj = get_sub( source_obj, key_pubsub_section );
            if ( pubsub_obj != null )
            {
                res.project_id   = get_json_string( pubsub_obj, key_project_id, dflt_project_id);
                res.subscript_id = get_json_string( pubsub_obj, key_subscript_id, dflt_subscript_id );
                res.receiver_max_rate = get_json_int( pubsub_obj, key_rec_max_rate, dflt_receiver_max_rate );
            }
        }
    }

    private static void read_result_settings( BLAST_SETTINGS res, JsonObject root )
    {
        JsonObject result_obj = get_sub( root, key_result_section );
        if ( result_obj != null )
        {
            JsonObject asn1_obj = get_sub( result_obj, key_asn1_section );                    
            if ( asn1_obj != null )
            {
                res.gs_result_bucket  = get_json_string( asn1_obj, key_gs_bucket, dflt_gs_bucket );
                res.gs_result_file    = get_json_string( asn1_obj, key_gs_file, dflt_gs_result_file );

                res.save_dir = get_json_string( root, key_save_dir, dflt_save_dir() );
            }

            JsonObject status_obj = get_sub( result_obj, key_status_section );                    
            if ( asn1_obj != null )
            {
                res.gs_status_bucket    = get_json_string( status_obj, key_gs_bucket, dflt_gs_bucket );
                res.gs_status_file      = get_json_string( status_obj, key_gs_file, dflt_gs_status_file );

                res.gs_status_running   = get_json_string( status_obj, key_gs_running, dflt_gs_running );
                res.gs_status_done      = get_json_string( status_obj, key_gs_done, dflt_gs_done );
                res.gs_status_error     = get_json_string( status_obj, key_gs_error, dflt_gs_error );
            }
        }
    }

    private static void read_log_settings( BLAST_SETTINGS res, JsonObject root )
    {
        JsonObject log_obj = get_sub( root, key_log_section );
        if ( log_obj != null )
        {
            res.log_request      = get_json_bool( log_obj, key_log_request, dflt_log_request );
            res.log_job_start    = get_json_bool( log_obj, key_log_start, dflt_log_start );
            res.log_job_done     = get_json_bool( log_obj, key_log_done, dflt_log_done );
            res.log_cutoff       = get_json_bool( log_obj, key_log_cutoff, dflt_log_cutoff );
            res.log_final        = get_json_bool( log_obj, key_log_final, dflt_log_final );
            res.log_part_prep    = get_json_bool( log_obj, key_log_part_prep, dflt_log_part_prep );
            res.log_worker_shift = get_json_bool( log_obj, key_log_worker_shift, dflt_log_worker_shift );
            res.log_pref_loc     = get_json_bool( log_obj, key_log_pref_loc, dflt_log_pref_loc );

            res.log_port = get_json_int( log_obj, key_log_port, dflt_log_port );
            res.log_host = get_host( log_obj, key_log_host, dflt_log_host );
        }
    }

    public static BLAST_SETTINGS read_from_json( final String json_file, final String appName )
    {
        BLAST_SETTINGS res = defaults( appName );

        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( new FileReader( json_file ) );
            if ( tree.isJsonObject() )
            {
                JsonObject root = tree.getAsJsonObject();

                res.appName = get_json_string( root, key_appName, appName );

                res.save_dir = get_json_string( root, key_save_dir, dflt_save_dir() );
                
                read_spark_settings( res, root );
                read_blastjni_settings( res, root );
                read_source_settings( res, root );
                read_result_settings( res, root );
                read_log_settings( res, root );
            }
        }           
        catch( Exception e )
        {
            System.out.println( String.format( "json-parsing: %s", e ) );
            res = defaults( appName );
        }
        return res;
    }
 
}
