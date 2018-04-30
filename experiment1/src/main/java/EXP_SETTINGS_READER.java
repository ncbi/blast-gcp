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

package gov.nih.nlm.ncbi.exp;

import java.util.List;
import java.util.ArrayList;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.FileReader;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

public class EXP_SETTINGS_READER
{
    // ------------------- sections in json-file ----------------------------------
    public static final String key_source_section = "source";
    public static final String key_socket_section = "socket";
    public static final String key_log_section = "log";
    public static final String key_spark_section = "spark";

    // ------------------- keys in json-file ---------------------------------------
    public static final String key_appName = "appName";
    public static final String key_batch_duration = "batch_duration";
    public static final String key_locality_wait = "locality_wait";
    public static final String key_with_locality = "with_locality";
    public static final String key_with_dyn_alloc = "with_dyn_alloc";
    public static final String key_host = "";
    public static final String key_port = "port";
    public static final String key_num_partitions = "num_partitions";
    public static final String key_num_executors = "num_executors";
    public static final String key_num_executor_cores = "num_executor_cores";
    public static final String key_executor_memory = "executor_memory";
    public static final String key_log_request = "request";
    public static final String key_log_cutoff = "cutoff";
    public static final String key_log_final = "final";
    public static final String key_log_pref_loc = "pref-loc";
    public static final String key_log_part_prep = "part-prep";
    public static final String key_log_prod1 = "prod1";
    public static final String key_log_prod2 = "prod2";

    // ------------------- default values -----------------------------------------
    public static final Integer dflt_batch_duration = 2;
    public static final String  dflt_locality_wait = "3s";
    public static final Boolean dflt_with_locality = false;
    public static final Boolean dflt_with_dyn_alloc = false;
    public static final String  dflt_host = "";
    public static final Integer dflt_port = 0; // log: 10011 trigger:10012
    public static final Integer dflt_num_partitions = 0;
    public static final Integer dflt_num_executors = 10;
    public static final Integer dflt_num_executor_cores = 5;
    public static final String  dflt_executor_memory = "";
    public static final Boolean dflt_log_request = true;
    public static final Boolean dflt_log_cutoff = true;
    public static final Boolean dflt_log_final = true;
    public static final Boolean dflt_log_pref_loc = false;
    public static final Boolean dflt_log_part_prep = false;
    public static final Boolean dflt_log_prod1 = true;
    public static final Boolean dflt_log_prod2 = true;


    public static EXP_SETTINGS defaults( final String appName )
    {
        EXP_SETTINGS res = new EXP_SETTINGS();
        res.appName = appName;
        
        res.batch_duration  = dflt_batch_duration;
        res.locality_wait   = dflt_locality_wait;
        res.with_locality   = dflt_with_locality;
        res.with_dyn_alloc  = dflt_with_dyn_alloc;

        res.log_host        = dflt_host;
        res.log_port        = dflt_port;
        res.log_request     = dflt_log_request;
        res.log_cutoff      = dflt_log_cutoff;
        res.log_final       = dflt_log_final;
        res.log_pref_loc    = dflt_log_pref_loc;
        res.log_part_prep   = dflt_log_part_prep;
        res.log_prod1       = dflt_log_prod1;
        res.log_prod2       = dflt_log_prod2;

        res.trigger_host    = dflt_host;
        res.trigger_port    = dflt_port;
        
        res.num_partitions  = dflt_num_partitions;

        res.num_executors       = dflt_num_executors;
        res.num_executor_cores  = dflt_num_executor_cores;
        res.executor_memory     = dflt_executor_memory;

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


    private static void read_spark_settings( EXP_SETTINGS res, JsonObject root )
    {
        JsonObject spark_obj = get_sub( root, key_spark_section );
        if ( spark_obj != null )
        {
            res.batch_duration = get_json_int( spark_obj, key_batch_duration, dflt_batch_duration );
            res.locality_wait  = get_json_string( spark_obj, key_locality_wait, dflt_locality_wait );
            res.with_locality  = get_json_bool( spark_obj, key_with_locality, dflt_with_locality );
            res.with_dyn_alloc = get_json_bool( spark_obj, key_with_dyn_alloc, dflt_with_dyn_alloc );
            res.num_executors       = get_json_int( spark_obj, key_num_executors, dflt_num_executors );
            res.num_executor_cores  = get_json_int( spark_obj, key_num_executor_cores, dflt_num_executor_cores );
            res.executor_memory     = get_json_string( spark_obj, key_executor_memory, dflt_executor_memory );
        }
    }

    private static void read_source_settings( EXP_SETTINGS res, JsonObject root )
    {
        JsonObject source_obj = get_sub( root, key_source_section );
        if ( source_obj != null )
        {

            JsonObject socket_obj = get_sub( source_obj, key_socket_section );
            if ( socket_obj != null )
            {
                res.trigger_port = get_json_int( socket_obj, key_port, dflt_port );
                res.trigger_host = get_host( socket_obj, key_host, dflt_host );
            }
        }
    }

    private static void read_log_settings( EXP_SETTINGS res, JsonObject root )
    {
        JsonObject log_obj = get_sub( root, key_log_section );
        if ( log_obj != null )
        {
            res.log_port = get_json_int( log_obj, key_port, dflt_port );
            res.log_host = get_host( log_obj, key_host, dflt_host );

            res.log_request = get_json_bool( log_obj, key_log_request, dflt_log_request );
            res.log_cutoff = get_json_bool( log_obj, key_log_cutoff, dflt_log_cutoff );
            res.log_final = get_json_bool( log_obj, key_log_final, dflt_log_final );
            res.log_pref_loc = get_json_bool( log_obj, key_log_pref_loc, dflt_log_pref_loc );
            res.log_part_prep = get_json_bool( log_obj, key_log_part_prep, dflt_log_part_prep );
            res.log_prod1 = get_json_bool( log_obj, key_log_prod1, dflt_log_prod1 );
            res.log_prod2 = get_json_bool( log_obj, key_log_prod2, dflt_log_prod2 );
        }
    }

    public static EXP_SETTINGS read_from_json( final String json_file, final String appName )
    {
        EXP_SETTINGS res = defaults( appName );

        try
        {
            JsonParser parser = new JsonParser();
            JsonElement tree = parser.parse( new FileReader( json_file ) );
            if ( tree.isJsonObject() )
            {
                JsonObject root = tree.getAsJsonObject();

                res.appName = get_json_string( root, key_appName, appName );
                res.num_partitions = get_json_int( root, key_num_partitions, dflt_num_partitions );                

                read_spark_settings( res, root );
                read_source_settings( res, root );
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
