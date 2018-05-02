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

import java.io.Serializable;

public class BLAST_SETTINGS implements Serializable
{
    public String appName;
    
    public String db_location;
    public String db_pattern;
    public String db_bucket;

    public Integer batch_duration;
    public String  locality_wait;
    public Boolean with_locality;
    public Boolean with_dyn_alloc;
    public String  spark_log_level;

    public String log_host;
    public Integer log_port;
    
    public String trigger_host;
    public Integer trigger_port;

    public String  save_dir;    
    public Integer num_db_partitions;
    public Integer receiver_max_rate;
    public Integer top_n;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;

    public Boolean flat_db_layout;
    public Boolean log_request;
    public Boolean log_job_start;
    public Boolean log_job_done;
    public Boolean log_cutoff;
    public Boolean log_final;
    public Boolean log_part_prep;
    public Boolean log_worker_shift;
    public Boolean log_pref_loc;
    public String  jni_log_level;

    public String project_id;
    public String subscript_id;
    public String gs_result_bucket;
    public String gs_result_file;

    public String gs_status_bucket;
    public String gs_status_file;
    public String gs_status_running;
    public String gs_status_done;
    public String gs_status_error;

    public Boolean valid()
    {
        if ( db_bucket.isEmpty() ) return false;
        if ( num_db_partitions == 0 ) return false;
        if ( top_n == 0 ) return false;
        if ( project_id.isEmpty() ) return false;
        if ( subscript_id.isEmpty() ) return false;
        if ( gs_result_bucket.isEmpty() ) return false;
        if ( gs_status_bucket.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( db_bucket.isEmpty() ) S = S + "db_bucket is missing\n";
        if ( num_db_partitions == 0 ) S = S + "num_db_partitions is 0\n";
        if ( top_n == 0 ) S = S + "top_n is 0\n";
        if ( project_id.isEmpty() ) S = S + "project_id is missing\n";
        if ( subscript_id.isEmpty() ) S = S + "subscript_id is missing\n";
        if ( gs_result_bucket.isEmpty() ) S = S + "gs_result_bucket is missing\n";
        if ( gs_status_bucket.isEmpty() ) S = S + "gs_status_bucket is missing\n";
        return S;
    }

    @Override public String toString()
    {
        String S = String.format( "appName ............ '%s'\n", appName );
        S  =  S +  String.format( "spark log level .... '%s'\n", spark_log_level );
        S  =  S +  String.format( "db_location ........ '%s'\n", db_location );
        S  =  S +  String.format( "db_pattern ......... '%s'\n", db_pattern );
        S  =  S +  String.format( "db_bucket .......... '%s'\n", db_bucket );
        S  =  S +  String.format( "pubsub-subscript ... '%s' : '%s'\n", project_id, subscript_id );
        S  =  S +  String.format( "GS result .......... '%s' : '%s'\n", gs_result_bucket, gs_result_file );
        S  =  S +  String.format( "GS status .......... '%s' : '%s'\n", gs_status_bucket, gs_status_file );
        S  =  S +  String.format( "GS status-codes  ... '%s', '%s', '%s'\n", gs_status_running, gs_status_done, gs_status_error );
        S  =  S +  String.format( "batch_duration ..... %d seconds\n", batch_duration );
        S  =  S +  String.format( "locality.wait ...... %s\n", locality_wait );
        S  =  S +  String.format( "with_locality ...... %s\n", Boolean.toString( with_locality ) );
        S  =  S +  String.format( "with_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S  =  S +  String.format( "log_host ........... %s:%d\n", log_host, log_port );
        S  =  S +  String.format( "trigger_host ....... %s:%d\n", trigger_host, trigger_port );
        S  =  S +  String.format( "save_dir ........... '%s'\n", save_dir );
        S  =  S +  String.format( "num_db_partitions .. %d\n", num_db_partitions );
        S  =  S +  String.format( "dflt top_n ......... %d\n", top_n );
        S  =  S +  String.format( "rec. max. rate ..... %d per second\n", receiver_max_rate );
        S  =  S +  String.format( "executors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "executor memory..... %s\n", executor_memory );
        S  =  S +  String.format( "flat db layout...... %s\n", Boolean.toString( flat_db_layout ) );
        S  =  S +  String.format( "jni-log-level ...... %s\n", jni_log_level );

        String S_log = "";
        if ( log_request )   S_log = S_log + "request ";
        if ( log_job_start ) S_log = S_log + "job_start ";
        if ( log_job_done )  S_log = S_log + "job_done ";
        if ( log_cutoff )    S_log = S_log + "cutoff ";
        if ( log_final )     S_log = S_log + "final ";
        if ( log_part_prep )     S_log = S_log + "part-prep ";
        if ( log_worker_shift )  S_log = S_log + "worker-shift ";
        if ( log_pref_loc )  S_log = S_log + "pref_log ";

        S  =  S +  String.format( "log ................ %s\n", S_log );
        return S;
    }
 
}
