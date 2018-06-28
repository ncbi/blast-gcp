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

class SOURCE_PUBSUB_SETTINGS_READER
{
    public static final String key = "pubsub";
    public static final String key_use = "use";
    public static final String key_project_id = "project_id";
    public static final String key_subscript_id = "subscript_id";

    public static final Boolean dflt_use = false;
    public static final String  dflt_project_id = "";
    public static final String  dflt_subscript_id = "";

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        settings.use_pubsub_source = SE_UTILS.get_json_bool( obj, key_use, dflt_use );
        settings.project_id   = SE_UTILS.get_json_string( obj, key_project_id, dflt_project_id );
        settings.subscript_id = SE_UTILS.get_json_string( obj, key_subscript_id, dflt_subscript_id );
    }
}

class SOURCE_HDFS_SETTINGS_READER
{
    public static final String key = "hdfs";
    public static final String key_use = "use";
    public static final String key_hdfs_source_dir = "dir";

    public static final Boolean dflt_use = false;
    public static final String dflt_hdfs_source_dir = "hdfs:///user/%s/jobs/";

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        settings.use_hdfs_source = SE_UTILS.get_json_bool( obj, key_use, dflt_use );
        settings.hdfs_source_dir = SE_UTILS.insert_username( SE_UTILS.get_json_string( obj, key_hdfs_source_dir, dflt_hdfs_source_dir ) );
    }
}

class SOURCE_SETTINGS_READER
{
    // ------------------- sections in json-file ----------------------------------
    public static final String key = "source";
    public static final String key_max_backlog = "max_backlog";
    public static final Integer dflt_max_backlog = 10;

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        SOURCE_PUBSUB_SETTINGS_READER.from_json( obj, settings );
        SOURCE_HDFS_SETTINGS_READER.from_json( obj, settings );
        settings.max_backlog = SE_UTILS.get_json_int( obj, key_max_backlog, dflt_max_backlog );
    }
}

class BLASTJNI_SETTINGS_READER
{
    public static final String key = "blastjni";
    public static final String key_db = "db";
    public static final String key_top_n = "top_n";
    public static final String key_num_db_limit = "num_db_limit";
    public static final String key_jni_log_level = "jni_log_level";
    public static final String key_manifest_root = "manifest_root";
    public static final String key_location = "location";
    public static final String key_num_locations = "num_locations";
    public static final String key_lib_name = "lib_name";

    public static final Integer dflt_top_n = 10;
    public static final Integer dflt_num_db_limit = 0;
    public static final String  dflt_jni_log_level = "INFO";
    public static final String  dflt_manifest_root = "";
    public static final String  dflt_location = "/tmp/blast/db";
    public static final Integer dflt_num_locations = 1;
    public static final String  dflt_lib_name = "libblastjni.so";

    private static void db_list_from_json( JsonObject obj, BLAST_SETTINGS settings )
    {
        JsonArray db_list_array = SE_UTILS.get_sub_array( obj, key_db );
        if ( db_list_array != null )
        {
            for ( JsonElement e : db_list_array ) 
            {
                if ( e.isJsonObject() )
                {
                    JsonObject o = e.getAsJsonObject();
                    BLAST_DB_SETTING db_setting = BLAST_DB_SETTING_READER.from_json( o,
                            settings.num_db_limit, settings.location, settings.num_locations );
                    settings.dbs.put( db_setting.key, db_setting );
                }
            }
        }
    }

    private static void db_list_from_conf( CONF conf, BLAST_SETTINGS settings )
    {
        for ( CONF_DATABASE e : conf.dbs )
        {
            BLAST_DB_SETTING db_setting = BLAST_DB_SETTING_READER.from_conf( e,
                            settings.num_db_limit, settings.location, settings.num_locations );
            settings.dbs.put( db_setting.key, db_setting );
        }
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.top_n         = SE_UTILS.get_json_int( obj, key_top_n, dflt_top_n );
            settings.num_db_limit  = SE_UTILS.get_json_int( obj, key_num_db_limit, dflt_num_db_limit );
            settings.num_locations = SE_UTILS.get_json_int( obj, key_num_locations, dflt_num_locations );
            settings.jni_log_level = SE_UTILS.get_json_string( obj, key_jni_log_level, dflt_jni_log_level );
            settings.manifest_root = SE_UTILS.get_json_string( obj, key_manifest_root, dflt_manifest_root );
            settings.location      = SE_UTILS.get_json_string( obj, key_location, dflt_location );
            settings.lib_name      = SE_UTILS.get_json_string( obj, key_lib_name, dflt_lib_name );

            if ( settings.manifest_root.isEmpty() )
            {
                /* we DO NOT have a manifest, the db-files are located as range... */
                db_list_from_json( obj, settings );
            }
            else
            {
                /* we DO have a manifest, the db-files are defined by the manifest... */
                CONF conf = CONF_READER.read_conf( settings.manifest_root );
                db_list_from_conf( conf, settings );
            }
        }
        else
        {
            settings.top_n         = dflt_top_n;
            settings.num_db_limit  = dflt_num_db_limit;
            settings.num_locations = dflt_num_locations;
            settings.jni_log_level = dflt_jni_log_level;
            settings.manifest_root = dflt_manifest_root;
            settings.location      = dflt_location;
            settings.lib_name      = dflt_lib_name;
        }
    }
}

class ASN1_SETTINGS_READER
{
    public static final String key = "asn1";
    public static final String key_gs_result_bucket = "bucket";
    public static final String key_gs_result_file = "file";
    public static final String key_hdfs_dir  = "hdfs_dir";
    public static final String key_hdfs_file = "hdfs_file";
    public static final String key_gs_or_hdfs = "gs_or_hdfs";

    public static final String  dflt_gs_result_bucket = "";
    public static final String  dflt_gs_result_file = "output/%s/seq-annot.asn";
    public static final String  dflt_hdfs_dir = "hdfs:///user/%s/results/";
    public static final String  dflt_hdfs_file = "%s.asn";
    public static final String  dflt_gs_or_hdfs = "gs";

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        settings.gs_result_bucket = SE_UTILS.get_json_string( obj, key_gs_result_bucket, dflt_gs_result_bucket );
        settings.gs_result_file   = SE_UTILS.get_json_string( obj, key_gs_result_file, dflt_gs_result_file );
        settings.hdfs_result_dir  = SE_UTILS.get_json_string( obj, key_hdfs_dir, SE_UTILS.insert_username( dflt_hdfs_dir ) );
        settings.hdfs_result_file = SE_UTILS.get_json_string( obj, key_hdfs_file, dflt_hdfs_file );
        settings.gs_or_hdfs       = SE_UTILS.get_json_string( obj, key_gs_or_hdfs, dflt_gs_or_hdfs );
    }
}

class STATUS_SETTINGS_READER
{
    public static final String key = "status";
    public static final String key_gs_bucket = "bucket";
    public static final String key_gs_file = "file";
    public static final String key_gs_running = "running";
    public static final String key_gs_done = "done";
    public static final String key_gs_error = "error";

    public static final String  dflt_gs_bucket = "";
    public static final String  dflt_gs_status_file = "status/%s/status.txt";
    public static final String  dflt_gs_running = "RUNNING";
    public static final String  dflt_gs_done = "DONE";
    public static final String  dflt_gs_error = "ERROR";

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        settings.gs_status_bucket  = SE_UTILS.get_json_string( obj, key_gs_bucket, dflt_gs_bucket );
        settings.gs_status_file    = SE_UTILS.get_json_string( obj, key_gs_file, dflt_gs_status_file );
        settings.gs_status_running = SE_UTILS.get_json_string( obj, key_gs_running, dflt_gs_running );
        settings.gs_status_done    = SE_UTILS.get_json_string( obj, key_gs_done, dflt_gs_done );
        settings.gs_status_error   = SE_UTILS.get_json_string( obj, key_gs_error, dflt_gs_error );
    }
}

class RESULTS_SETTINGS_READER
{
    public static final String key = "result";

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = SE_UTILS.get_sub( root, key );
        ASN1_SETTINGS_READER.from_json( obj, settings );
        STATUS_SETTINGS_READER.from_json( obj, settings );
    }
}

class SPARK_SETTINGS_READER
{
    public static final String key = "spark";
    public static final String key_transfer_files = "transfer_files";
    public static final String key_spark_log_level = "log_level";
    public static final String key_locality_wait = "locality_wait";
    public static final String key_with_locality = "with_locality";
    public static final String key_with_dyn_alloc = "with_dyn_alloc";
    public static final String key_num_executors = "num_executors";
    public static final String key_num_executor_cores = "num_executor_cores";
    public static final String key_executor_memory = "executor_memory";
    public static final String key_shuffle_reduceLocality_enabled = "shuffle_reduceLocality_enabled";
    public static final String key_scheduler_fair = "scheduler_fair";
    public static final String key_parallel_jobs = "parallel_jobs";
    public static final String key_comm_port = "comm_port";

    public static final String  dflt_transfer_file = "libblastjni.so";
    public static final String  dflt_spark_log_level = "ERROR";
    public static final String  dflt_locality_wait = "3s";
    public static final Boolean dflt_with_locality = true;
    public static final Boolean dflt_with_dyn_alloc = false;
    public static final Integer dflt_num_executors = 8;
    public static final Integer dflt_num_executor_cores = 2;
    public static final String  dflt_executor_memory = "";
    public static final Boolean dflt_shuffle_reduceLocality_enabled = false;
    public static final Boolean dflt_scheduler_fair = false;
    public static final Integer dflt_parallel_jobs = 6;
    public static final Integer dflt_comm_port = 10013;

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        settings.transfer_files     = new ArrayList<>();
        JsonObject obj = SE_UTILS.get_sub( root, key );
        SE_UTILS.get_string_list( obj, key_transfer_files, dflt_transfer_file, settings.transfer_files );
        settings.spark_log_level    = SE_UTILS.get_json_string( obj, key_spark_log_level, dflt_spark_log_level );
        settings.locality_wait      = SE_UTILS.get_json_string( obj, key_locality_wait, dflt_locality_wait );
        settings.with_locality      = SE_UTILS.get_json_bool( obj, key_with_locality, dflt_with_locality );
        settings.with_dyn_alloc     = SE_UTILS.get_json_bool( obj, key_with_dyn_alloc, dflt_with_dyn_alloc );
        settings.num_executors      = SE_UTILS.get_json_int( obj, key_num_executors, dflt_num_executors );
        settings.num_executor_cores = SE_UTILS.get_json_int( obj, key_num_executor_cores, dflt_num_executor_cores );
        settings.executor_memory    = SE_UTILS.get_json_string( obj, key_executor_memory, dflt_executor_memory );
        settings.shuffle_reduceLocality_enabled = SE_UTILS.get_json_bool( obj, key_shuffle_reduceLocality_enabled,
                                                                       dflt_shuffle_reduceLocality_enabled );
        settings.scheduler_fair     = SE_UTILS.get_json_bool( obj, key_scheduler_fair, dflt_scheduler_fair );
        settings.parallel_jobs      = SE_UTILS.get_json_int( obj, key_parallel_jobs, dflt_parallel_jobs );
        settings.comm_port          = SE_UTILS.get_json_int( obj, key_comm_port, dflt_comm_port );
    }
}

public class BLAST_SETTINGS_READER
{
    // ------------------- keys in json-file ---------------------------------------
    public static final String key_appName = "appName";

    public static BLAST_SETTINGS read_from_json( final String json_file, final String appName )
    {
        BLAST_SETTINGS res = new BLAST_SETTINGS();

        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( new FileReader( json_file ) );
            JsonObject root = null;
            if ( tree.isJsonObject() )
                root = tree.getAsJsonObject();

            res.appName = SE_UTILS.get_json_string( root, key_appName, appName );

            SOURCE_SETTINGS_READER.from_json( root, res );
            BLASTJNI_SETTINGS_READER.from_json( root, res );
            RESULTS_SETTINGS_READER.from_json( root, res );
            SPARK_SETTINGS_READER.from_json( root, res );
            BLAST_LOG_SETTING_READER.from_json( SE_UTILS.get_sub( root, BLAST_LOG_SETTING_READER.key ), res.log, res.jni_log_level );
        }           
        catch( Exception e )
        {
            System.out.println( String.format( "json-parsing: %s", e ) );
        }
        return res;
    }

    public static SparkConf configure( BLAST_SETTINGS settings )
    {
        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( settings.with_dyn_alloc ) );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );
        conf.set( "spark.locality.wait", settings.locality_wait );
        conf.set( "spark.shuffle.reduceLocality.enabled", Boolean.toString( settings.shuffle_reduceLocality_enabled ) );

        if ( settings.scheduler_fair )
        {
            conf.set( "spark.scheduler.mode", "FAIR" );
            conf.set( "spark.scheduler.allocation.file", "./pooles.xml" );
        }

        conf.set( "spark.yarn.executor.memoryOverhead", "1024" );
        return conf;
    }
}
