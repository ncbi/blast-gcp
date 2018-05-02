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

class UTILS
{
    public static String get_json_string( JsonObject root, final String key, final String dflt )
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

    public static Integer get_json_int( JsonObject root, final String key, final Integer dflt )
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

    public static Boolean get_json_bool( JsonObject root, final String key, final Boolean dflt )
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

    public static JsonObject get_sub( JsonObject root, final String key )
    {
        JsonElement e = root.get( key );
        if ( e != null )
        {
            if ( e.isJsonObject() )
                return e.getAsJsonObject();
        }
        return null;
    }

    public static String get_host( JsonObject root, final String key, final String dflt )
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

    public static String insert_username( final String pattern )
    {
        final String username = System.getProperty( "user.name" );
        return String.format( pattern, username );
        //return String.format( "hdfs:///user/%s/results/", username );
    }
}

class SOURCE_PUBSUB_SETTINGS_READER
{
    public static final String key = "pubsub";
    public static final String key_use = "use";
    public static final String key_project_id = "project_id";
    public static final String key_subscript_id = "subscript_id";

    public static final Boolean dflt_use = false;
    public static final String  dflt_project_id = "";
    public static final String  dflt_subscript_id = "";

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.project_id         = dflt_project_id;
        settings.subscript_id       = dflt_subscript_id;
        settings.use_pubsub_source  = dflt_use;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.use_pubsub_source = UTILS.get_json_bool( obj, key_use, dflt_use );
            settings.project_id   = UTILS.get_json_string( obj, key_project_id, dflt_project_id );
            settings.subscript_id = UTILS.get_json_string( obj, key_subscript_id, dflt_subscript_id );
        }
        else
            settings.use_pubsub_source = false;
    }
}

class SOURCE_SOCKET_SETTINGS_READER
{
    public static final String key = "socket";
    public static final String key_use = "use";
    public static final String key_trigger_port = "trigger_port";
    public static final String key_trigger_host = "trigger_host";

    public static final Boolean dflt_use = false;
    public static final String  dflt_trigger_host = "";
    public static final Integer dflt_trigger_port = 10012;

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.use_socket_source = dflt_use;
        settings.trigger_port = dflt_trigger_port;
        settings.trigger_host = dflt_trigger_host;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.use_socket_source = UTILS.get_json_bool( obj, key_use, dflt_use );
            settings.trigger_port = UTILS.get_json_int( obj, key_trigger_port, dflt_trigger_port );
            settings.trigger_host = UTILS.get_host( obj, key_trigger_host, dflt_trigger_host );
        }
        else
            settings.use_socket_source = false;
    }
}

class SOURCE_HDFS_SETTINGS_READER
{
    public static final String key = "hdfs";
    public static final String key_use = "use";
    public static final String key_hdfs_source_dir = "dir";

    public static final Boolean dflt_use = false;
    public static final String dflt_hdfs_source_dir = "hdfs:///user/%s/jobs/";

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.use_hdfs_source = dflt_use;
        settings.hdfs_source_dir = UTILS.insert_username( dflt_hdfs_source_dir );
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.use_hdfs_source = UTILS.get_json_bool( obj, key_use, dflt_use );
            settings.hdfs_source_dir = UTILS.insert_username( UTILS.get_json_string( obj, key_hdfs_source_dir, dflt_hdfs_source_dir ) );
        }
        else
            settings.use_hdfs_source = false;
    }
}

class SOURCE_SETTINGS_READER
{
    // ------------------- sections in json-file ----------------------------------
    public static final String key = "source";
    public static final String key_rec_max_rate = "receiver_max_rate";
    public static final Integer dflt_receiver_max_rate = 5;

    public static void defaults( BLAST_SETTINGS settings )
    {
        SOURCE_PUBSUB_SETTINGS_READER.defaults( settings );
        SOURCE_SOCKET_SETTINGS_READER.defaults( settings );
        SOURCE_HDFS_SETTINGS_READER.defaults( settings );
        settings.receiver_max_rate  = dflt_receiver_max_rate;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            SOURCE_PUBSUB_SETTINGS_READER.from_json( obj, settings );
            SOURCE_SOCKET_SETTINGS_READER.from_json( obj, settings );
            SOURCE_HDFS_SETTINGS_READER.from_json( obj, settings );
            settings.receiver_max_rate  = UTILS.get_json_int( obj, key_rec_max_rate, dflt_receiver_max_rate );
        }
    }
}

class DB_SETTINGS_READER
{
    public static final String key = "db";
    public static final String key_db_location = "db_location";
    public static final String key_db_pattern = "db_pattern";
    public static final String key_db_bucket = "db_bucket";
    public static final String key_flat_db_layout = "flat_db_layout";
    public static final String key_num_db_partitions = "num_db_partitions";

    public static final String  dflt_db_location = "/tmp/blast/db";
    public static final String  dflt_db_pattern = "nt_50M";
    public static final String  dflt_db_bucket = ""; // "nt_50mb_chunks";
    public static final Boolean dflt_flat_db_layout = false;
    public static final Integer dflt_num_db_partitions = 0;

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.db_location = dflt_db_location;
        settings.db_pattern  = dflt_db_pattern;
        settings.db_bucket   = dflt_db_bucket;
        settings.flat_db_layout     = dflt_flat_db_layout;
        settings.num_db_partitions  = dflt_num_db_partitions;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.db_location = UTILS.get_json_string( obj, key_db_location, dflt_db_location );
            settings.db_pattern  = UTILS.get_json_string( obj, key_db_pattern, dflt_db_pattern );
            settings.db_bucket   = UTILS.get_json_string( obj, key_db_bucket, dflt_db_bucket );
            settings.flat_db_layout    = UTILS.get_json_bool( obj, key_flat_db_layout, dflt_flat_db_layout );
            settings.num_db_partitions = UTILS.get_json_int( obj, key_num_db_partitions, dflt_num_db_partitions );
        }
    }
}

class BLASTJNI_SETTINGS_READER
{
    public static final String key = "blastjni";
    public static final String key_top_n = "top_n";
    public static final String key_jni_log_level = "jni_log_level";

    public static final Integer dflt_top_n = 10;
    public static final String  dflt_jni_log_level = "Info";

    public static void defaults( BLAST_SETTINGS settings )
    {
        DB_SETTINGS_READER.defaults( settings );
        settings.top_n         = dflt_top_n;
        settings.jni_log_level = dflt_jni_log_level;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            DB_SETTINGS_READER.from_json( obj, settings );
            settings.top_n         = UTILS.get_json_int( obj, key_top_n, dflt_top_n );
            settings.jni_log_level = UTILS.get_json_string( obj, key_jni_log_level, dflt_jni_log_level );
        }
    }
}

class ASN1_SETTINGS_READER
{
    public static final String key = "asn1";
    public static final String key_gs_bucket = "bucket";
    public static final String key_gs_file = "file";
    public static final String key_hdfs_dir  = "hdfs_dir";
    public static final String key_hdfs_file = "hdfs_file";

    public static final String  dflt_gs_bucket = "";
    public static final String  dflt_gs_result_file = "output/%s/seq-annot.asn";
    public static final String  dflt_hdfs_dir = "hdfs:///user/%s/results/";
    public static final String  dflt_hdfs_file = "%s.asn";

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.gs_result_bucket = dflt_gs_bucket;
        settings.gs_result_file   = dflt_gs_result_file;
        settings.hdfs_result_dir  = UTILS.insert_username( dflt_hdfs_dir );
        settings.hdfs_result_file = dflt_hdfs_file;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.gs_result_bucket = UTILS.get_json_string( obj, key_gs_bucket, dflt_gs_bucket );
            settings.gs_result_file   = UTILS.get_json_string( obj, key_gs_file, dflt_gs_result_file );
            settings.hdfs_result_dir  = UTILS.get_json_string( obj, key_hdfs_dir, UTILS.insert_username( dflt_hdfs_dir ) );
            settings.hdfs_result_file = UTILS.get_json_string( obj, key_hdfs_file, dflt_hdfs_file );
        }
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

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.gs_status_bucket  = dflt_gs_bucket;
        settings.gs_status_file    = dflt_gs_status_file;
        settings.gs_status_running = dflt_gs_running;
        settings.gs_status_done    = dflt_gs_done;
        settings.gs_status_error   = dflt_gs_error;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.gs_status_bucket  = UTILS.get_json_string( obj, key_gs_bucket, dflt_gs_bucket );
            settings.gs_status_file    = UTILS.get_json_string( obj, key_gs_file, dflt_gs_status_file );
            settings.gs_status_running = UTILS.get_json_string( obj, key_gs_running, dflt_gs_running );
            settings.gs_status_done    = UTILS.get_json_string( obj, key_gs_done, dflt_gs_done );
            settings.gs_status_error   = UTILS.get_json_string( obj, key_gs_error, dflt_gs_error );
        }
    }
}

class RESULTS_SETTINGS_READER
{
    public static final String key = "result";

    public static void defaults( BLAST_SETTINGS settings )
    {
        ASN1_SETTINGS_READER.defaults( settings );
        STATUS_SETTINGS_READER.defaults( settings );
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            ASN1_SETTINGS_READER.from_json( obj, settings );
            STATUS_SETTINGS_READER.from_json( obj, settings );
        }
    }
}

class SPARK_SETTINGS_READER
{
    public static final String key = "spark";
    public static final String key_spark_log_level = "log_level";
    public static final String key_batch_duration = "batch_duration";
    public static final String key_locality_wait = "locality_wait";
    public static final String key_with_locality = "with_locality";
    public static final String key_with_dyn_alloc = "with_dyn_alloc";
    public static final String key_num_executors = "num_executors";
    public static final String key_num_executor_cores = "num_executor_cores";
    public static final String key_executor_memory = "executor_memory";

    public static final String  dflt_spark_log_level = "ERROR";
    public static final Integer dflt_batch_duration = 2;
    public static final String  dflt_locality_wait = "3s";
    public static final Boolean dflt_with_locality = false;
    public static final Boolean dflt_with_dyn_alloc = false;
    public static final Integer dflt_num_executors = 10;
    public static final Integer dflt_num_executor_cores = 5;
    public static final String  dflt_executor_memory = "";

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.spark_log_level    = dflt_spark_log_level;
        settings.batch_duration     = dflt_batch_duration;
        settings.locality_wait      = dflt_locality_wait;
        settings.with_locality      = dflt_with_locality;
        settings.with_dyn_alloc     = dflt_with_dyn_alloc;
        settings.num_executors      = dflt_num_executors;
        settings.num_executor_cores = dflt_num_executor_cores;
        settings.executor_memory    = dflt_executor_memory;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.spark_log_level    = UTILS.get_json_string( obj, key_spark_log_level, dflt_spark_log_level );
            settings.batch_duration     = UTILS.get_json_int( obj, key_batch_duration, dflt_batch_duration );
            settings.locality_wait      = UTILS.get_json_string( obj, key_locality_wait, dflt_locality_wait );
            settings.with_locality      = UTILS.get_json_bool( obj, key_with_locality, dflt_with_locality );
            settings.with_dyn_alloc     = UTILS.get_json_bool( obj, key_with_dyn_alloc, dflt_with_dyn_alloc );
            settings.num_executors      = UTILS.get_json_int( obj, key_num_executors, dflt_num_executors );
            settings.num_executor_cores = UTILS.get_json_int( obj, key_num_executor_cores, dflt_num_executor_cores );
            settings.executor_memory    = UTILS.get_json_string( obj, key_executor_memory, dflt_executor_memory );
        }
    }
}

class LOG_SETTINGS_READER
{
    public static final String key = "log";
    public static final String key_log_host = "log_host";
    public static final String key_log_port = "log_port";
    public static final String key_log_request = "log_request";
    public static final String key_log_start = "log_start";
    public static final String key_log_done = "log_done";
    public static final String key_log_cutoff = "log_cutoff";
    public static final String key_log_final = "log_final";
    public static final String key_log_part_prep = "log_partition_prep";
    public static final String key_log_worker_shift = "log_worker_shift";
    public static final String key_log_pref_loc = "log_pref_loc";

    public static final String  dflt_log_host = "";
    public static final Integer dflt_log_port = 0; //10011;
    public static final Boolean dflt_log_request = true;
    public static final Boolean dflt_log_start = false;
    public static final Boolean dflt_log_done = false;
    public static final Boolean dflt_log_cutoff = true;
    public static final Boolean dflt_log_final = true;
    public static final Boolean dflt_log_part_prep = false;
    public static final Boolean dflt_log_worker_shift = false;
    public static final Boolean dflt_log_pref_loc = false;

    public static void defaults( BLAST_SETTINGS settings )
    {
        settings.log_host           = dflt_log_host;
        settings.log_port           = dflt_log_port;

        settings.log_request        = dflt_log_request;
        settings.log_job_start      = dflt_log_start;
        settings.log_job_done       = dflt_log_done;
        settings.log_cutoff         = dflt_log_cutoff;
        settings.log_final          = dflt_log_final;
        settings.log_part_prep      = dflt_log_part_prep;
        settings.log_worker_shift   = dflt_log_worker_shift;
        settings.log_pref_loc       = dflt_log_pref_loc;
    }

    public static void from_json( JsonObject root, BLAST_SETTINGS settings )
    {
        JsonObject obj = UTILS.get_sub( root, key );
        if ( obj != null )
        {
            settings.log_port         = UTILS.get_json_int( obj, key_log_port, dflt_log_port );
            settings.log_host         = UTILS.get_host( obj, key_log_host, dflt_log_host );

            settings.log_request      = UTILS.get_json_bool( obj, key_log_request, dflt_log_request );
            settings.log_job_start    = UTILS.get_json_bool( obj, key_log_start, dflt_log_start );
            settings.log_job_done     = UTILS.get_json_bool( obj, key_log_done, dflt_log_done );
            settings.log_cutoff       = UTILS.get_json_bool( obj, key_log_cutoff, dflt_log_cutoff );
            settings.log_final        = UTILS.get_json_bool( obj, key_log_final, dflt_log_final );
            settings.log_part_prep    = UTILS.get_json_bool( obj, key_log_part_prep, dflt_log_part_prep );
            settings.log_worker_shift = UTILS.get_json_bool( obj, key_log_worker_shift, dflt_log_worker_shift );
            settings.log_pref_loc     = UTILS.get_json_bool( obj, key_log_pref_loc, dflt_log_pref_loc );
        }
    }
}

public class BLAST_SETTINGS_READER
{
    // ------------------- keys in json-file ---------------------------------------
    public static final String key_appName = "appName";

    public static BLAST_SETTINGS defaults( final String appName )
    {
        BLAST_SETTINGS res = new BLAST_SETTINGS();
        res.appName = appName;

        SOURCE_SETTINGS_READER.defaults( res );
        BLASTJNI_SETTINGS_READER.defaults( res );
        RESULTS_SETTINGS_READER.defaults( res );
        SPARK_SETTINGS_READER.defaults( res );
        LOG_SETTINGS_READER.defaults( res );

        return res;
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
                res.appName = UTILS.get_json_string( root, key_appName, appName );

                SOURCE_SETTINGS_READER.from_json( root, res );
                BLASTJNI_SETTINGS_READER.from_json( root, res );
                RESULTS_SETTINGS_READER.from_json( root, res );
                SPARK_SETTINGS_READER.from_json( root, res );
                LOG_SETTINGS_READER.from_json( root, res );
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
