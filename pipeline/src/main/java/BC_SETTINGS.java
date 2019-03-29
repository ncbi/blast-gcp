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
import java.util.HashMap;

/**
 * utility-class to store all application-settings
 * implemented as a singleton
 *
*/
public final class BC_SETTINGS
{
    public String appName = "";

    /* REQUESTS */
    public boolean req_use_pubsub = false;
    public String req_pubsub_project_id = "";
    public String req_pubsub_subscript_id = "";
    public boolean req_use_files = false;
    public String req_files_dir = "";
    public boolean req_use_socket = false;
    public int req_port_nr = 0;
    public int req_max_backlog = 10;
    public int job_sleep_time = 100;

    /* DATABASES */
    HashMap< String, BC_DATABASE_SETTING > dbs; // configured via ini.json section

    /* RESULTS */
    public boolean res_use_gs_bucket = false;
    public String res_gs_bucket = "";
    public String res_gs_pattern = "";
    public boolean res_use_files = false;
    public String res_files_dir = "";
    public String res_files_pattern = "";
    public String report_dir = "report";

    /* CLUSTER */
    public List< String > transfer_files;
    public String  spark_log_level = "INFO";
    public String  locality_wait = "24h";
    public boolean set_dyn_alloc = false;
    public boolean with_dyn_alloc = false;
    public String  executor_memory = "";
    public boolean set_shuffle_reduceLocality = false;
    public boolean shuffle_reduceLocality = false;
    public boolean scheduler_fair = false;
    public boolean map_partitions = false;
    public int num_executors = 0;
    public int num_executor_cores = 0;
    public int parallel_jobs = 1;
    public String jni_log_level = "INFO";
    public int console_sleep_time = 200;
    public int debug_receiver_sleep_time = 200;

    /* DEBUG */
    BC_DEBUG_SETTINGS debug;

/**
 * create instance of BC_SETTINGS with all values set to default
 *
 * @see     BC_DEBUG_SETTINGS
*/
    public BC_SETTINGS()
    {
        dbs = new HashMap<>();
        transfer_files = new ArrayList<>();
        debug = new BC_DEBUG_SETTINGS();
    }

/**
 * test if the request-settings are valid
 *
 * @return      are request-settings valid ?
*/
    private boolean req_valid()
    {
        /*
        >>> when code for these special request-sources has been added
            these test will make sense

        boolean res = req_use_pubsub || req_use_files || req_use_socket;
        if ( req_use_pubsub )
            res = !req_pubsub_project_id.isEmpty() &&  !req_pubsub_subscript_id.isEmpty();
        if ( res && req_use_pubsub )
            res = !req_files_dir.isEmpty();
        if ( res && req_use_socket )
            res = req_port_nr != 0;
        return res;
        */
        return true;
    }

/**
 * test if the database-settings are valid
 *
 * @return      are database-settings valid ?
*/
    private boolean dbs_valid()
    {
        if ( dbs.isEmpty() ) return false;
        boolean res = true;
        for ( BC_DATABASE_SETTING e : dbs.values() )
            if ( !e.valid() ) res = false;
        return res;
    }

/**
 * test if result-settings are valid
 *
 * @return      are result-settings valid ?
*/
    private boolean res_valid()
    {
        /*
        >>> when code for these special result-destinations has been added
            these test will make sense

        boolean res = res_use_gs_bucket || res_use_files;
        if ( res_use_gs_bucket )
            res = !res_gs_bucket.isEmpty() && !res_gs_pattern.isEmpty();
        if ( res && res_use_files )
            res = !res_files_dir.isEmpty() && !res_files_pattern.isEmpty();
        return res;
        */
        return true;
    }

/**
 * test if the given transfer-files can be found
 *
 * @return      transfer-files found ?
*/
    private boolean transfer_files_present()
    {
        int not_found = 0;
        for ( String S : transfer_files )
        {
            if ( !BC_UTILS.file_exists( S ) )
                not_found += 1;
        }
        return ( not_found == 0 );
    }

/**
 * test if all settings are valid
 *
 * @return      are all settings valid ?
*/
    public boolean valid()
    {
        return req_valid() && dbs_valid() && res_valid() && transfer_files_present();
    }

/**
 * convert all settings to a string
 *
 * @return      string containing all settings
*/
    @Override public String toString()
    {
        String S = "REQUESTS:\n";
        if ( req_use_pubsub )
            S = S + String.format( "\tpubsub-subscript ... '%s':'%s'\n", req_pubsub_project_id, req_pubsub_subscript_id );
        if ( req_use_files )
            S = S + String.format( "\tfiles-dir .......... '%s'\n", req_files_dir );
        if ( req_use_socket )
            S = S + String.format( "\tport ............... %d\n", req_port_nr );
        S = S + String.format( "\tmax. backlog ......... %d requests\n", req_max_backlog );

        S = S + "\nDATABASES:\n";
        for ( BC_DATABASE_SETTING e : dbs.values() )
            S = S + e.toString();

        if ( res_use_gs_bucket || res_use_files )
            S = S + "\nRESULTS:\n";
        if ( res_use_gs_bucket )
            S = S + String.format( "\tbucket ............. %s/%s\n", res_gs_bucket, res_gs_pattern );
        if ( res_use_files )
            S = S + String.format( "\tfiles .............. %s/%s\n", res_files_dir, res_files_pattern );

        S = S + "\nCLUSTER:\n";
        S = S + String.format( "\tappName ............ '%s'\n", appName );
        S = S + String.format( "\ttransfer files ..... %s\n", transfer_files );
        S = S + String.format( "\tspark log level .... '%s'\n", spark_log_level );
        if ( !locality_wait.isEmpty() )
            S = S + String.format( "\tlocality.wait ...... '%s'\n", locality_wait );
        if ( set_dyn_alloc )
            S = S + String.format( "\twith_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S = S + String.format( "\tmap partitions ..... %s\n", Boolean.toString( map_partitions ) );

        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "\texecutor memory..... %s\n", executor_memory );
        if ( set_shuffle_reduceLocality )
        S = S + String.format( "\treduce_loc ......... %s\n", Boolean.toString( shuffle_reduceLocality ) );

        S = S + String.format( "\tscheduler fair ..... %s\n", Boolean.toString( scheduler_fair ) );
        if ( num_executors > 0 )
            S = S + String.format( "\tnum-executors ...... %d\n", num_executors );
        if ( num_executor_cores > 0 )
            S = S + String.format( "\tnum-executor-cores . %d\n", num_executor_cores );
        S = S + String.format( "\tparallel jobs ...... %d\n", parallel_jobs );
        S = S + String.format( "\tjni log level ...... '%s'\n", jni_log_level );

        if ( debug.events_selected() )
        {
            S = S + "\nDEBUG:\n";
            S = S + debug.toString();
        }

        return S;
    }
}

