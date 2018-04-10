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

public class BLAST_SETTINGS
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
    public static final String key_num_workers = "num_workers";
    public static final String key_rec_max_rate = "receiver_max_rate";
    public static final String key_top_n = "top_n";
    public static final String key_num_executors = "num_executors";
    public static final String key_num_executor_cores = "num_executor_cores";
    public static final String key_executor_memory = "executor_memory";
    public static final String key_flat_db_layout = "flat_db_layout";
    public static final String key_log_request = "log_request";
    public static final String key_log_start = "log_start";
    public static final String key_log_done = "log_done";
    public static final String key_log_final = "log_final";
    public static final String key_project_id = "project_id";
    public static final String key_subscript_id = "subscript_id";

    // ------------------- default values -----------------------------------------
    public static final String  dflt_db_location = "/tmp/blast/db";
    public static final String  dflt_db_pattern = "nt_50M";
    public static final Integer dflt_batch_duration = 10;
    public static final String  dflt_log_host = "localhost";
    public static final Integer dflt_log_port = 10011;
    public static final String  dflt_trigger_host = "localhost";
    public static final Integer dflt_trigger_port = 10012;
    public static final Integer dflt_num_db_partitions = 10;
    public static final Integer dflt_num_workers = 2;
    public static final Integer dflt_receiver_max_rate = 5;
    public static final Integer dflt_top_n = 10;
    public static final Integer dflt_num_executors = 10;
    public static final Integer dflt_num_executor_cores = 5;
    public static final String  dflt_executor_memory = "";
    public static final Boolean dflt_flat_db_layout = false;
    public static final Boolean dflt_log_request = false;
    public static final Boolean dflt_log_start = false;
    public static final Boolean dflt_log_done = false;
    public static final Boolean dflt_log_final = true;
    public static final String  dflt_project_id = "ncbi-sandbox-blast";
    public static final String  dflt_subscript_id = "spart-test-subscript";

    // ----------------------------------------------------------------------------
    public String appName;
    
    public String db_location;
    public String db_pattern;
    
    public Integer batch_duration;
    public List< String > files_to_transfer;
    
    public String log_host;
    public Integer log_port;
    
    public String trigger_host;
    public Integer trigger_port;

    public String  save_dir;    
    public Integer num_db_partitions;
    public Integer num_workers;
    public Integer receiver_max_rate;
    public Integer top_n;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;

    public Boolean flat_db_layout;
    public Boolean log_request;
    public Boolean log_job_start;
    public Boolean log_job_done;
    public Boolean log_final;   

    public String project_id;
    public String subscript_id;

    private String dflt_save_dir()
    {
        final String username = System.getProperty( "user.name" );
        return String.format( "hdfs:///user/%s/results/", username );
    }

    public BLAST_SETTINGS( final String appName )
    {
        this.appName = appName;
        
        db_location = dflt_db_location;
        db_pattern  = dflt_db_pattern;
        
        batch_duration = dflt_batch_duration;
        files_to_transfer = new ArrayList<>();
        
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            String local_host = localMachine.getHostName();
            log_host     = local_host;
            trigger_host = local_host;
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            log_host = dflt_log_host;
            trigger_host = dflt_trigger_host;
        }
        log_port = dflt_log_port;
        trigger_port = dflt_trigger_port;
        
        save_dir = dflt_save_dir();
        
        num_db_partitions   = dflt_num_db_partitions;
        num_workers         = dflt_num_workers;
        receiver_max_rate   = dflt_receiver_max_rate;
        top_n               = dflt_top_n;

        num_executors       = dflt_num_executors;
        num_executor_cores  = dflt_num_executor_cores;
        executor_memory     = dflt_executor_memory;

        flat_db_layout      = dflt_flat_db_layout;

        log_request         = dflt_log_request;
        log_job_start       = dflt_log_start;
        log_job_done        = dflt_log_done;
        log_final           = dflt_log_final;

        project_id      = dflt_project_id;
        subscript_id    = dflt_subscript_id;
    }
    
    public BLAST_SETTINGS( final BLAST_INI ini_file, final String appName )
    {
        final String ini_section = "APP";
        this.appName = ini_file.getString( ini_section, key_appName, appName );
        
        db_location = ini_file.getString( ini_section, key_db_location, dflt_db_location );
        db_pattern  = ini_file.getString( ini_section, key_db_pattern, dflt_db_pattern );
        
        batch_duration = ini_file.getInt( ini_section, key_batch_duration, dflt_batch_duration );

        files_to_transfer = new ArrayList<>();

        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            String local_host = localMachine.getHostName();
            log_host     = ini_file.getString( ini_section, key_log_host, local_host );
            trigger_host = ini_file.getString( ini_section, key_trigger_host, local_host );
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            log_host = ini_file.getString( ini_section, key_log_host, dflt_log_host );
            trigger_host = ini_file.getString( ini_section, key_trigger_host, dflt_trigger_host );
        }
        
        log_port     = ini_file.getInt( ini_section, key_log_port, dflt_log_port );
        trigger_port = ini_file.getInt( ini_section, key_trigger_port, dflt_trigger_port );
        
        save_dir = ini_file.getString( ini_section, key_save_dir, dflt_save_dir() );
        
        num_db_partitions   = ini_file.getInt( ini_section, key_num_db_partitions, dflt_num_db_partitions );
        num_workers         = ini_file.getInt( ini_section, key_num_workers, dflt_num_workers );
        receiver_max_rate   = ini_file.getInt( ini_section, key_rec_max_rate, dflt_receiver_max_rate );
        top_n               = ini_file.getInt( ini_section, key_top_n, dflt_top_n );

        num_executors       = ini_file.getInt( ini_section, key_num_executors, dflt_num_executors );
        num_executor_cores  = ini_file.getInt( ini_section, key_num_executor_cores, dflt_num_executor_cores );
        executor_memory     = ini_file.getString( ini_section, key_executor_memory, dflt_executor_memory );

        flat_db_layout      = ini_file.getBoolean( ini_section, key_flat_db_layout, dflt_flat_db_layout );

        log_request     = ini_file.getBoolean( ini_section, key_log_request, dflt_log_request );
        log_job_start   = ini_file.getBoolean( ini_section, key_log_start, dflt_log_start );
        log_job_done    = ini_file.getBoolean( ini_section, key_log_done, dflt_log_done );
        log_final       = ini_file.getBoolean( ini_section, key_log_final, dflt_log_final );

        project_id      = ini_file.getString( ini_section, key_project_id, dflt_project_id);
        subscript_id    = ini_file.getString( ini_section, key_subscript_id, dflt_subscript_id );
    }

    @Override public String toString()
    {
        String S = String.format( "appName ............ %s\n", appName );
        S  =  S +  String.format( "db_location ........ %s\n", db_location );
        S  =  S +  String.format( "db_pattern ......... %s\n", db_pattern );
        S  =  S +  String.format( "project_id ......... %s\n", project_id );
        S  =  S +  String.format( "subscript_id ....... %s\n", subscript_id );
        S  =  S +  String.format( "batch_duration ..... %d seconds\n", batch_duration );
        S  =  S +  String.format( "log_host ........... %s:%d\n", log_host, log_port );
        S  =  S +  String.format( "trigger_host ....... %s:%d\n", trigger_host, trigger_port );
        S  =  S +  String.format( "save_dir ........... %s\n", save_dir );
        S  =  S +  String.format( "num_db_partitions .. %d\n", num_db_partitions );
        S  =  S +  String.format( "num_workers ........ %d\n", num_workers );
        S  =  S +  String.format( "top_n .............. %d\n", top_n );
        S  =  S +  String.format( "rec. max. rate ..... %d per second\n", receiver_max_rate );
        S  =  S +  String.format( "executors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "executor memory..... %s\n", executor_memory );
        S  =  S +  String.format( "flat db layout...... %s\n", Boolean.toString( flat_db_layout ) );
        S  =  S +  String.format( "log_request ........ %s\n", Boolean.toString( log_request ) );
        S  =  S +  String.format( "log_job_start ...... %s\n", Boolean.toString( log_job_start ) );
        S  =  S +  String.format( "log_job_done ....... %s\n", Boolean.toString( log_job_done ) );
        S  =  S +  String.format( "log_final........... %s\n", Boolean.toString( log_final ) );
        return S;
    }
    
}
