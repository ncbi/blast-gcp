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
    // ------------------- keys in INI-file ---------------------------------------
    public static final String key_appName = "appName";
    public static final String key_db_location = "db_location";
    public static final String key_db_pattern = "db_pattern";
    public static final String key_batch_duration = "batch_duration";
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
    public static final String key_project_id = "project_id";
    public static final String key_subscript_id = "subscript_id";
    public static final String key_gs_result_bucket = "result_bucket";
    public static final String key_gs_result_file = "result_file";

    // ------------------- default values -----------------------------------------
    public static final String  dflt_db_location = "/tmp/blast/db";
    public static final String  dflt_db_pattern = "nt_50M";
    public static final Integer dflt_batch_duration = 10;
    public static final String  dflt_log_host = "localhost";
    public static final Integer dflt_log_port = 10011;
    public static final String  dflt_trigger_host = "localhost";
    public static final Integer dflt_trigger_port = 10012;
    public static final Integer dflt_num_db_partitions = 10;
    public static final Integer dflt_receiver_max_rate = 5;
    public static final Integer dflt_top_n = 10;
    public static final Integer dflt_num_executors = 10;
    public static final Integer dflt_num_executor_cores = 5;
    public static final String  dflt_executor_memory = "";
    public static final Boolean dflt_flat_db_layout = false;
    public static final Boolean dflt_log_request = false;
    public static final Boolean dflt_log_start = false;
    public static final Boolean dflt_log_done = false;
    public static final Boolean dflt_log_cutoff = true;
    public static final Boolean dflt_log_final = true;
    public static final String  dflt_project_id = "ncbi-sandbox-blast";
    public static final String  dflt_subscript_id = "spark-test-subscript";
    public static final String  dflt_gs_result_bucket = "blastgcp-pipeline-test";
    public static final String  dflt_gs_result_file = "output/%s/seq-annot.asn";

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
        
        res.batch_duration = dflt_batch_duration;
        
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            String local_host = localMachine.getHostName();
            res.log_host     = local_host;
            res.trigger_host = local_host;
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            res.log_host = dflt_log_host;
            res.trigger_host = dflt_trigger_host;
        }
        res.log_port = dflt_log_port;
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

        res.project_id          = dflt_project_id;
        res.subscript_id        = dflt_subscript_id;
        res.gs_result_bucket    = dflt_gs_result_bucket;
        res.gs_result_file      = dflt_gs_result_file;
        
        return res;
    }
    
    public static BLAST_SETTINGS read_from_ini( final BLAST_INI ini_file, final String appName )
    {
        BLAST_SETTINGS res = new BLAST_SETTINGS();

        final String ini_section = "APP";
        res.appName = ini_file.getString( ini_section, key_appName, appName );
        
        res.db_location = ini_file.getString( ini_section, key_db_location, dflt_db_location );
        res.db_pattern  = ini_file.getString( ini_section, key_db_pattern, dflt_db_pattern );
        
        res.batch_duration = ini_file.getInt( ini_section, key_batch_duration, dflt_batch_duration );

        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            String local_host = localMachine.getHostName();
            res.log_host     = ini_file.getString( ini_section, key_log_host, local_host );
            res.trigger_host = ini_file.getString( ini_section, key_trigger_host, local_host );
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            res.log_host = ini_file.getString( ini_section, key_log_host, dflt_log_host );
            res.trigger_host = ini_file.getString( ini_section, key_trigger_host, dflt_trigger_host );
        }
        
        res.log_port     = ini_file.getInt( ini_section, key_log_port, dflt_log_port );
        res.trigger_port = ini_file.getInt( ini_section, key_trigger_port, dflt_trigger_port );
        
        res.save_dir = ini_file.getString( ini_section, key_save_dir, dflt_save_dir() );
        
        res.num_db_partitions   = ini_file.getInt( ini_section, key_num_db_partitions, dflt_num_db_partitions );
        res.receiver_max_rate   = ini_file.getInt( ini_section, key_rec_max_rate, dflt_receiver_max_rate );
        res.top_n               = ini_file.getInt( ini_section, key_top_n, dflt_top_n );

        res.num_executors       = ini_file.getInt( ini_section, key_num_executors, dflt_num_executors );
        res.num_executor_cores  = ini_file.getInt( ini_section, key_num_executor_cores, dflt_num_executor_cores );
        res.executor_memory     = ini_file.getString( ini_section, key_executor_memory, dflt_executor_memory );

        res.flat_db_layout      = ini_file.getBoolean( ini_section, key_flat_db_layout, dflt_flat_db_layout );

        res.log_request     = ini_file.getBoolean( ini_section, key_log_request, dflt_log_request );
        res.log_job_start   = ini_file.getBoolean( ini_section, key_log_start, dflt_log_start );
        res.log_job_done    = ini_file.getBoolean( ini_section, key_log_done, dflt_log_done );
        res.log_cutoff      = ini_file.getBoolean( ini_section, key_log_cutoff, dflt_log_cutoff );
        res.log_final       = ini_file.getBoolean( ini_section, key_log_final, dflt_log_final );

        res.project_id      = ini_file.getString( ini_section, key_project_id, dflt_project_id);
        res.subscript_id    = ini_file.getString( ini_section, key_subscript_id, dflt_subscript_id );
        res.gs_result_bucket = ini_file.getString( ini_section, key_gs_result_bucket, dflt_gs_result_bucket );
        res.gs_result_file  = ini_file.getString( ini_section, key_gs_result_file, dflt_gs_result_file );

        return res;
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

    private static Boolean get_json_bool( JsonObject root, final String name, final Boolean dflt )
    {
        Boolean res = dflt;
        JsonElement elem = root.get( name );
        if ( elem != null )
        {
            try
            {
                res = elem.getAsBoolean();
            }
            catch( Exception e )
            {
            }
        }
        return res;
    }

    public static BLAST_SETTINGS read_from_json( final String json_file, final String appName )
    {
        BLAST_SETTINGS res = new BLAST_SETTINGS();
        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( new FileReader( json_file ) );
            if ( tree.isJsonObject() )
            {
                JsonObject root = tree.getAsJsonObject();

                res.appName = get_json_string( root, key_appName, appName );

                res.db_location = get_json_string( root, key_db_location, dflt_db_location );
                res.db_pattern  = get_json_string( root, key_db_pattern, dflt_db_pattern );
        
                res.batch_duration = get_json_int( root, key_batch_duration, dflt_batch_duration );

                try
                {
                    java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
                    String local_host = localMachine.getHostName();
                    res.log_host     = get_json_string( root, key_log_host, local_host );
                    res.trigger_host = get_json_string( root, key_trigger_host, local_host );
                }
                catch ( UnknownHostException e )
                {
                    System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
                    res.log_host     = get_json_string( root, key_log_host, dflt_log_host );
                    res.trigger_host = get_json_string( root, key_trigger_host, dflt_trigger_host );
                }
                
                res.log_port     = get_json_int( root, key_log_port, dflt_log_port );
                res.trigger_port = get_json_int( root, key_trigger_port, dflt_trigger_port );

                res.save_dir = get_json_string( root, key_save_dir, dflt_save_dir() );
                
                res.num_db_partitions   = get_json_int( root, key_num_db_partitions, dflt_num_db_partitions );
                res.receiver_max_rate   = get_json_int( root, key_rec_max_rate, dflt_receiver_max_rate );
                res.top_n               = get_json_int( root, key_top_n, dflt_top_n );

                res.num_executors       = get_json_int( root, key_num_executors, dflt_num_executors );
                res.num_executor_cores  = get_json_int( root, key_num_executor_cores, dflt_num_executor_cores );
                res.executor_memory     = get_json_string( root, key_executor_memory, dflt_executor_memory );

                res.flat_db_layout      = get_json_bool( root, key_flat_db_layout, dflt_flat_db_layout );

                res.log_request     = get_json_bool( root, key_log_request, dflt_log_request );
                res.log_job_start   = get_json_bool( root, key_log_start, dflt_log_start );
                res.log_job_done    = get_json_bool( root, key_log_done, dflt_log_done );
                res.log_cutoff      = get_json_bool( root, key_log_cutoff, dflt_log_cutoff );
                res.log_final       = get_json_bool( root, key_log_final, dflt_log_final );

                res.project_id      = get_json_string( root, key_project_id, dflt_project_id);
                res.subscript_id    = get_json_string( root, key_subscript_id, dflt_subscript_id );
                res.gs_result_bucket = get_json_string( root, key_gs_result_bucket, dflt_gs_result_bucket );
                res.gs_result_file  = get_json_string( root, key_gs_result_file, dflt_gs_result_file );

            }
        }           
        catch( Exception e )
        {
            res = defaults( appName );
        }
        return res;
    }
 
}
