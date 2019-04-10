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
import java.io.FileReader;

import org.apache.spark.SparkConf;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.stream.JsonReader;

/**
 * reads the pubsub-section of the requests-section of the settings-json-file
 *
*/
class REQUESTS_PUBSUB_SETTINGS_READER
{
    private static final String key = "pubsub";
    private static final String key_use = "use";
    private static final String key_project_id = "project_id";
    private static final String key_subscript_id = "subscript_id";

/**
 * extracts all pubsub-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.req_use_pubsub = BC_JSON_UTILS.get_json_bool( obj, key_use,
                settings.req_use_pubsub);
            settings.req_pubsub_project_id = BC_JSON_UTILS.get_json_string( obj, key_project_id,
                settings.req_pubsub_project_id );
            settings.req_pubsub_subscript_id = BC_JSON_UTILS.get_json_string( obj, key_subscript_id,
                settings.req_pubsub_subscript_id );
        }
    }
}

/**
 * reads the files-section of the requests-section of the settings-json-file
 *
*/
class REQUESTS_FILES_SETTINGS_READER
{
    private static final String key = "files";
    private static final String key_use = "use";
    private static final String key_dir = "dir";

/**
 * extracts all files-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.req_use_files = BC_JSON_UTILS.get_json_bool( obj, key_use, settings.req_use_files );
            settings.req_files_dir = BC_JSON_UTILS.get_json_string( obj, key_dir, settings.req_files_dir );
        }
    }
}

/**
 * reads the socket-section of the requests-section of the settings-json-file
 *
*/
class REQUESTS_SOCKET_SETTINGS_READER
{
    private static final String key = "socket";
    private static final String key_use = "use";
    private static final String key_port = "port";

/**
 * extracts all socket-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.req_use_socket = BC_JSON_UTILS.get_json_bool( obj, key_use,
                settings.req_use_socket );
            settings.req_port_nr = BC_JSON_UTILS.get_json_int( obj, key_port,
                settings.req_port_nr );
        }
    }
}

/**
 * reads the requests-section of the settings-json-file
 *
*/
class REQUESTS_SETTINGS_READER
{
    // ------------------- sections in json-file ----------------------------------
    private static final String key = "requests";
    private static final String key_max_backlog = "max_backlog";

/**
 * extracts all requests-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
 * @see     REQUESTS_PUBSUB_SETTINGS_READER
 * @see     REQUESTS_FILES_SETTINGS_READER
 * @see     REQUESTS_SOCKET_SETTINGS_READER
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            REQUESTS_PUBSUB_SETTINGS_READER.from_json( obj, settings );
            REQUESTS_FILES_SETTINGS_READER.from_json( obj, settings );
            REQUESTS_SOCKET_SETTINGS_READER.from_json( obj, settings );
            settings.req_max_backlog = BC_JSON_UTILS.get_json_int( obj, key_max_backlog,
                settings.req_max_backlog );
        }
    }
}

/**
 * reads the database-section of the settings-json-file
 *
*/
class DATABASES_SETTINGS_READER
{
    private static final String key = "databases";
    private static final String key_db = "key";
    private static final String key_w_loc = "worker_location";
    private static final String key_s_loc = "source_location";
    private static final String key_ext = "extensions";
    private static final String key_direct = "direct";
    private static final String key_limit = "limit";

/**
 * extracts all database-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_DATABASE_SETTING
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonArray db_list_array = BC_JSON_UTILS.get_sub_array( root, key );
        if ( db_list_array != null )
        {
            for ( JsonElement e : db_list_array )
            {
                if ( e.isJsonObject() )
                {
                    JsonObject obj = e.getAsJsonObject();
                    BC_DATABASE_SETTING db_settings = new BC_DATABASE_SETTING();

                    db_settings.key = BC_JSON_UTILS.get_json_string( obj, key_db,
                        db_settings.key );
                    db_settings.worker_location = BC_JSON_UTILS.get_json_string( obj,
                        key_w_loc, db_settings.worker_location );
                    db_settings.source_location = BC_JSON_UTILS.get_json_string( obj,
                        key_s_loc, db_settings.source_location );
                    BC_JSON_UTILS.get_string_list( obj, key_ext, "",
                        db_settings.extensions );
                    db_settings.direct = BC_JSON_UTILS.get_json_bool( obj,
                        key_direct, db_settings.direct );
                    db_settings.limit = BC_JSON_UTILS.get_json_int( obj,
                        key_limit, db_settings.limit );

                    if ( !db_settings.key.isEmpty() )
                        settings.dbs.put( db_settings.key, db_settings );
                }
            }
        }
    }
}

/**
 * reads the result-bucket-section of the settings-json-file
 *
*/
class RESULTS_BUCKET_SETTINGS_READER
{
    private static final String key = "bucket";
    private static final String key_use = "use";
    private static final String key_bucket = "bucket";
    private static final String key_pattern = "pattern";

/**
 * extracts all bucket-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.res_use_gs_bucket = BC_JSON_UTILS.get_json_bool( obj,
                key_use, settings.res_use_gs_bucket  );
            settings.res_gs_bucket = BC_JSON_UTILS.get_json_string( obj,
                key_bucket, settings.res_gs_bucket );
            settings.res_gs_pattern = BC_JSON_UTILS.get_json_string( obj,
                key_pattern, settings.res_gs_pattern );
        }
    }
}

/**
 * reads the result-files-section of the settings-json-file
 *
*/
class RESULTS_FILES_SETTINGS_READER
{
    private static final String key = "files";
    private static final String key_use = "use";
    private static final String key_dir = "dir";
    private static final String key_pattern = "pattern";

/**
 * extracts all files-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.res_use_files = BC_JSON_UTILS.get_json_bool( obj,
                key_use, settings.res_use_files );
            settings.res_files_dir = BC_JSON_UTILS.get_json_string( obj,
                key_dir, settings.res_files_dir );
            settings.res_files_pattern = BC_JSON_UTILS.get_json_string( obj,
                key_pattern, settings.res_files_pattern );
        }
    }
}

/**
 * reads the result-section of the settings-json-file
 *
*/
class RESULTS_SETTINGS_READER
{
    private static final String key = "results";

/**
 * extracts all result-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
 * @see     RESULTS_BUCKET_SETTINGS_READER
 * @see     RESULTS_FILES_SETTINGS_READER
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            RESULTS_BUCKET_SETTINGS_READER.from_json( obj, settings );
            RESULTS_FILES_SETTINGS_READER.from_json( obj, settings );
        }
    }
}

/**
 * reads the cluster-section of the settings-json-file
 *
*/
class CLUSTER_SETTINGS_READER
{
    private static final String key = "cluster";
    private static final String key_transfer_files = "transfer_files";
    private static final String key_spark_log_level = "log_level";
    private static final String key_locality_wait = "locality_wait";
    private static final String key_set_dyn_alloc = "set_dyn_alloc";
    private static final String key_with_dyn_alloc = "with_dyn_alloc";
    private static final String key_executor_memory = "executor_memory";
    private static final String key_set_shuffle_reduceLocality = "set_shuffle_reduceLocality";
    private static final String key_shuffle_reduceLocality = "shuffle_reduceLocality";
    private static final String key_scheduler_fair = "scheduler_fair";
    private static final String key_num_executors = "num_executors";
    private static final String key_num_executor_cores = "num_executor_cores";
    private static final String key_parallel_jobs = "parallel_jobs";
    private static final String key_jni_log_level = "jni_log_level";
    private static final String  dflt_transfer_file = "libblastjni.so";
    private static final String key_predownload_dbs = "predownload_dbs";
    private static final boolean dflt_predownload_dbs = false;

/**
 * extracts all cluster-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   settings    BC_SETTINGS-instance to write into
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_SETTINGS settings )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            BC_JSON_UTILS.get_string_list( obj, key_transfer_files, dflt_transfer_file,
                settings.transfer_files );
            settings.spark_log_level = BC_JSON_UTILS.get_json_string( obj,
                key_spark_log_level, settings.spark_log_level );
            settings.locality_wait = BC_JSON_UTILS.get_json_string( obj,
                key_locality_wait, settings.locality_wait );
            settings.set_dyn_alloc = BC_JSON_UTILS.get_json_bool( obj,
                key_set_dyn_alloc, settings.set_dyn_alloc );
            settings.with_dyn_alloc = BC_JSON_UTILS.get_json_bool( obj,
                key_with_dyn_alloc, settings.with_dyn_alloc );
            settings.executor_memory = BC_JSON_UTILS.get_json_string( obj,
                key_executor_memory, settings.executor_memory );
            settings.set_shuffle_reduceLocality = BC_JSON_UTILS.get_json_bool( obj,
                key_set_shuffle_reduceLocality, settings.set_shuffle_reduceLocality );
            settings.shuffle_reduceLocality = BC_JSON_UTILS.get_json_bool( obj,
                key_shuffle_reduceLocality, settings.shuffle_reduceLocality );
            settings.scheduler_fair = BC_JSON_UTILS.get_json_bool( obj,
                key_scheduler_fair, settings.scheduler_fair );
            settings.num_executors = BC_JSON_UTILS.get_json_int( obj,
                key_num_executors, settings.num_executors );
            settings.num_executor_cores = BC_JSON_UTILS.get_json_int( obj,
                key_num_executor_cores, settings.num_executor_cores );
            settings.parallel_jobs = BC_JSON_UTILS.get_json_int( obj,
                key_parallel_jobs, settings.parallel_jobs);
            settings.jni_log_level = BC_JSON_UTILS.get_json_string( obj,
                key_jni_log_level, settings.jni_log_level );
            settings.predownload_dbs = BC_JSON_UTILS.get_json_bool( obj,
                key_predownload_dbs, dflt_predownload_dbs );
        }
    }
}

/**
 * reads the debug-section of the settings-json-file
 *
*/
class DEBUG_SETTINGS_READER
{
    private static final String key = "debug";
    private static final String key_port = "port";
    private static final String key_request = "request";
    private static final String key_start = "start";
    private static final String key_done = "done";
    private static final String key_final = "final";
    private static final String key_part_prep = "part_prep";
    private static final String key_worker_shift = "worker_shift";
    private static final String key_pref_loc = "pref_loc";
    private static final String key_db_copy = "db_copy";
    private static final String key_req_file_add = "req_file_add";
    private static final String key_req_add = "req_add";
    private static final String key_avg_time = "avg_time";

/**
 * extracts all cluster-settings from the JsonObject
 *
 * @param   root        JsonObject to extract values from
 * @param   setting     BC_DEBUG_SETTINGS-instance to write into
 * @param   a_jni_log_level jni-log-level to be written into the debug-settings
 * @see     BC_JSON_UTILS
 * @see     BC_SETTINGS
*/
    public static void from_json( JsonObject root, BC_DEBUG_SETTINGS setting, final String a_jni_log_level )
    {
        JsonObject obj = BC_JSON_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            setting.port         = BC_JSON_UTILS.get_json_int( obj, key_port, setting.port );
            setting.host         = BC_UTILS.get_local_host( setting.host );

            setting.request      = BC_JSON_UTILS.get_json_bool( obj, key_request, setting.request );
            setting.job_start    = BC_JSON_UTILS.get_json_bool( obj, key_start, setting.job_start);
            setting.job_done     = BC_JSON_UTILS.get_json_bool( obj, key_done, setting.job_done );
            setting.log_final    = BC_JSON_UTILS.get_json_bool( obj, key_final, setting.log_final );
            setting.part_prep    = BC_JSON_UTILS.get_json_bool( obj, key_part_prep, setting.part_prep );
            setting.worker_shift = BC_JSON_UTILS.get_json_bool( obj, key_worker_shift, setting.worker_shift );
            setting.pref_loc     = BC_JSON_UTILS.get_json_bool( obj, key_pref_loc, setting.pref_loc );
            setting.db_copy      = BC_JSON_UTILS.get_json_bool( obj, key_db_copy, setting.db_copy );
            setting.req_file_added = BC_JSON_UTILS.get_json_bool( obj, key_req_file_add, setting.req_file_added );
            setting.req_added      = BC_JSON_UTILS.get_json_bool( obj, key_req_add, setting.req_added );
            setting.avg_time       = BC_JSON_UTILS.get_json_bool( obj, key_avg_time, setting.avg_time  );
        }
        else
            setting.host = BC_UTILS.get_local_host( setting.host );

        setting.jni_log_level = a_jni_log_level;
    }
}

/**
 * reads all sections of the the settings-json-file
 *
*/
public final class BC_SETTINGS_READER
{
    // ------------------- keys in json-file ---------------------------------------
    public static final String key_appName = "appName";

/**
 * extracts all settings from the given json-file
 *
 * @param   json_file   path of file to be parsed into settings
 * @param   appName     application name to be written into the settings
 * @see     BC_SETTINGS
 * @see     BC_JSON_UTILS
 * @see     REQUESTS_SETTINGS_READER
 * @see     DATABASES_SETTINGS_READER
 * @see     RESULTS_SETTINGS_READER
 * @see     CLUSTER_SETTINGS_READER
 * @see     DEBUG_SETTINGS_READER
*/
    public static BC_SETTINGS read_from_json( final String json_file, final String appName )
    {
        BC_SETTINGS res = new BC_SETTINGS();

        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( new FileReader( json_file ) );
            if ( tree.isJsonObject() )
            {
                JsonObject root = tree.getAsJsonObject();

                res.appName = BC_JSON_UTILS.get_json_string( root, key_appName, appName );
                REQUESTS_SETTINGS_READER.from_json( root, res );
                DATABASES_SETTINGS_READER.from_json( root, res );
                RESULTS_SETTINGS_READER.from_json( root, res );
                CLUSTER_SETTINGS_READER.from_json( root, res );
                DEBUG_SETTINGS_READER.from_json( root, res.debug, res.jni_log_level );
            }
        }           
        catch( Exception e )
        {
            System.out.println( String.format( "json-parsing: %s", e ) );
        }
        return res;
    }

/**
 * creates a spark-context and configures it based on the given settings
 *
 * @param   settings    BC_SETTINGS-instance to be used to configure the spark-context
 *
 * @see     BC_SETTINGS
*/
    public static SparkConf createSparkConfAndConfigure( BC_SETTINGS settings )
    {
        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        if ( !settings.locality_wait.isEmpty() )
            conf.set( "spark.locality.wait", settings.locality_wait );

        if ( settings.set_dyn_alloc )
            conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( settings.with_dyn_alloc ) );

        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );

        if ( settings.set_shuffle_reduceLocality )
            conf.set( "spark.shuffle.reduceLocality.enabled", Boolean.toString( settings.shuffle_reduceLocality ) );

        if ( settings.scheduler_fair )
        {
            conf.set( "spark.scheduler.mode", "FAIR" );
            conf.set( "spark.scheduler.allocation.file", "./pooles.xml" );
        }

        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        else
            conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( true ) );

        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );

        return conf;
    }
}

