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
    public Boolean log_sing_request;

    public String project_id;
    public String subscript_id;
    public String gs_result_bucket;
    public String gs_result_file;

    @Override public String toString()
    {
        String S = String.format( "appName ............ '%s'\n", appName );
        S  =  S +  String.format( "db_location ........ '%s'\n", db_location );
        S  =  S +  String.format( "db_pattern ......... '%s'\n", db_pattern );
        S  =  S +  String.format( "db_bucket .......... '%s'\n", db_bucket );
        S  =  S +  String.format( "pubsub-subscript ... '%s' : '%s'\n", project_id, subscript_id );
        S  =  S +  String.format( "GS result-bucket ... '%s'\n", gs_result_bucket );
        S  =  S +  String.format( "GS result-file ..... '%s'\n", gs_result_file );
        S  =  S +  String.format( "batch_duration ..... %d seconds\n", batch_duration );
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

        String S_log = "";
        if ( log_request )   S_log = S_log + "request ";
        if ( log_job_start ) S_log = S_log + "job_start ";
        if ( log_job_done )  S_log = S_log + "job_done ";
        if ( log_cutoff )    S_log = S_log + "cutoff ";
        if ( log_final )     S_log = S_log + "final ";
        if ( log_part_prep )     S_log = S_log + "part-prep ";
        if ( log_worker_shift )  S_log = S_log + "worker-shift ";
        if ( log_sing_request )  S_log = S_log + "singleton-requests ";

        S  =  S +  String.format( "log ................ %s\n", S_log );
        return S;
    }
 
}
