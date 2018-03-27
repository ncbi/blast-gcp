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

import java.util.*;
import java.io.*;
import java.net.*;

public class GCP_BLAST_SETTINGS
{
    // --------------------keys and constants -------------------------------------
    public static final String appName_key = "appName";
    public static final String default_bucket = "nt_50mb_chunks";
    public static final String bucket_key = "bucket";
    
    public static final Integer default_batch_duration = 10;
    public static final Integer default_log_port = 10011;
    public static final Integer default_trigger_port = 10012;
    public static final Integer default_num_db_partitions = 10;
    public static final Integer default_num_job_partitions = 1;
    
    // ----------------------------------------------------------------------------
    public String appName;
    public String bucket;
    public Integer batch_duration;
    public List< String > files_to_transfer;
    
    public String log_host;
    public Integer log_port;
    
    public String trigger_host;
    public Integer trigger_port;
    
    public String save_dir;
    public Integer num_db_partitions;
    public Integer num_job_partitions;
    public Boolean log_request;
    public Boolean log_job_start;
    public Boolean log_job_done;
    public Boolean log_final;   

    public GCP_BLAST_SETTINGS( final String appName )
    {
        this.appName = appName;
        bucket = default_bucket;
        batch_duration = default_batch_duration;
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
            log_host = "";
        }
        log_port = default_log_port;
        trigger_port = default_trigger_port;
        
        final String username = System.getProperty( "user.name" );
        save_dir = String.format( "hdfs:///user/%s/results/", username );
        
        num_db_partitions = default_num_db_partitions;
        num_job_partitions = default_num_job_partitions;
        
        log_request = true;
        log_job_start = false;
        log_job_done = false;
        log_final = true;
    }
    
    public GCP_BLAST_SETTINGS( final GCP_BLAST_INI ini_file, final String appName )
    {
        final String ini_section = "APP";
        this.appName = ini_file.getString( ini_section, appName_key, appName );
        bucket = ini_file.getString( ini_section, bucket_key, default_bucket );
        batch_duration = ini_file.getInt( ini_section, "batch_duration", default_batch_duration );

        files_to_transfer = new ArrayList<>();

        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            String local_host = localMachine.getHostName();
            log_host     = ini_file.getString( ini_section, "log_host", local_host );
            trigger_host = ini_file.getString( ini_section, "trigger_host", local_host );
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            log_host = ini_file.getString( ini_section, "log_host", "localhost" );
        }
        
        log_port     = ini_file.getInt( ini_section, "log_port", default_log_port );
        trigger_port = ini_file.getInt( ini_section, "trigger_port", default_trigger_port );
        
        final String username = System.getProperty( "user.name" );
        
        save_dir = String.format( "hdfs:///user/%s/results/", username );
        save_dir = ini_file.getString( ini_section, "save_dir", save_dir );
        
        num_db_partitions   = ini_file.getInt( ini_section, "num_db_partitions", default_num_db_partitions );
        num_job_partitions  = ini_file.getInt( ini_section, "num_job_partitions", default_num_job_partitions );
        
        log_request     = ini_file.getBoolean( ini_section, "log_request", true );
        log_job_start   = ini_file.getBoolean( ini_section, "log_start", false );
        log_job_done    = ini_file.getBoolean( ini_section, "log_done", false );
        log_final       = ini_file.getBoolean( ini_section, "log_final", true );
    }

    @Override public String toString()
    {
        String S = String.format( "appName ............ %s\n", appName );
        S  =  S +  String.format( "batch_duration ..... %d\n", batch_duration );
        S  =  S +  String.format( "log_host ........... %s\n", log_host );
        S  =  S +  String.format( "log_port ........... %d\n", log_port );
        S  =  S +  String.format( "trigger_host ....... %s\n", trigger_host );
        S  =  S +  String.format( "trigger_port ....... %d\n", trigger_port );
        S  =  S +  String.format( "save_dir ........... %s\n", save_dir );
        S  =  S +  String.format( "num_db_partitions .. %s\n", num_db_partitions );
        S  =  S +  String.format( "num_job_partitions . %s\n", num_job_partitions );
        S  =  S +  String.format( "log_request ........ %s\n", Boolean.toString( log_request ) );
        S  =  S +  String.format( "log_job_start ...... %s\n", Boolean.toString( log_job_start ) );
        S  =  S +  String.format( "log_job_done ....... %s\n", Boolean.toString( log_job_done ) );
        S  =  S +  String.format( "log_final........... %s\n", Boolean.toString( log_final ) );
        return S;
    }
    
}
