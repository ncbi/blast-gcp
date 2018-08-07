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

import com.google.gson.JsonObject;

public class BLAST_LOG_SETTING_READER
{
    public static final String key = "log";
    public static final String key_host = "host";
    public static final String key_port = "port";
    public static final String key_request = "request";
    public static final String key_start = "start";
    public static final String key_done = "done";
    public static final String key_cutoff = "cutoff";
    public static final String key_final = "final";
    public static final String key_part_prep = "part_prep";
    public static final String key_worker_shift = "worker_shift";
    public static final String key_pref_loc = "pref_loc";
    public static final String key_db_copy = "db_copy";
    public static final String key_req_file_add = "req_file_add";
    public static final String key_req_add = "req_add";
    public static final String key_avg_time = "avg_time";
    public static final String key_stkdrv_stats_log = "stkdrv_stats_log";
    public static final String key_stkdrv_stats_app = "stkdrv_stats_app";
    public static final String key_stkdrv_stats_res = "stkdrv_stats_res";

    public static final String  dflt_host = "";
    public static final Integer dflt_port = 10011;
    public static final Boolean dflt_request = true;
    public static final Boolean dflt_start = false;
    public static final Boolean dflt_done = false;
    public static final Boolean dflt_cutoff = false;
    public static final Boolean dflt_final = true;
    public static final Boolean dflt_part_prep = false;
    public static final Boolean dflt_worker_shift = false;
    public static final Boolean dflt_pref_loc = false;
    public static final Boolean dflt_db_copy = false;
    public static final Boolean dflt_req_file_add = false;
    public static final Boolean dflt_req_add = false;
    public static final Boolean dflt_avg_time = false;

    public static final String  dflt_stkdrv_stats_log = "projects/ncbi-sandbox-blast/logs/dataproc-job-stats";
    public static final String  dflt_stkdrv_stats_app = "blast-gcp";
    public static final String  dflt_stkdrv_stats_res = "global";

    public static void defaults( BLAST_LOG_SETTING setting )
    {
        setting.host           = dflt_host;
        setting.port           = dflt_port;

        setting.request        = dflt_request;
        setting.job_start      = dflt_start;
        setting.job_done       = dflt_done;
        setting.cutoff         = dflt_cutoff;
        setting.log_final      = dflt_final;
        setting.part_prep      = dflt_part_prep;
        setting.worker_shift   = dflt_worker_shift;
        setting.pref_loc       = dflt_pref_loc;
        setting.db_copy        = dflt_db_copy;
        setting.req_file_added = dflt_req_file_add;
        setting.req_added      = dflt_req_add;
        setting.avg_time       = dflt_avg_time;

		setting.stkdrv_stats_log	= dflt_stkdrv_stats_log;
		setting.stkdrv_stats_app	= dflt_stkdrv_stats_app;
		setting.stkdrv_stats_res	= dflt_stkdrv_stats_res;
    }

    public static void from_json( JsonObject obj, BLAST_LOG_SETTING setting, final String a_jni_log_level )
    {
        if ( obj != null )
        {
            setting.port         = SE_UTILS.get_json_int( obj, key_port, dflt_port );
            setting.host         = SE_UTILS.get_host( obj, key_host, dflt_host );

            setting.request      = SE_UTILS.get_json_bool( obj, key_request, dflt_request );
            setting.job_start    = SE_UTILS.get_json_bool( obj, key_start, dflt_start );
            setting.job_done     = SE_UTILS.get_json_bool( obj, key_done, dflt_done );
            setting.cutoff       = SE_UTILS.get_json_bool( obj, key_cutoff, dflt_cutoff );
            setting.log_final    = SE_UTILS.get_json_bool( obj, key_final, dflt_final );
            setting.part_prep    = SE_UTILS.get_json_bool( obj, key_part_prep, dflt_part_prep );
            setting.worker_shift = SE_UTILS.get_json_bool( obj, key_worker_shift, dflt_worker_shift );
            setting.pref_loc     = SE_UTILS.get_json_bool( obj, key_pref_loc, dflt_pref_loc );
            setting.db_copy      = SE_UTILS.get_json_bool( obj, key_db_copy, dflt_db_copy );
            setting.req_file_added = SE_UTILS.get_json_bool( obj, key_req_file_add, dflt_req_file_add );
            setting.req_added      = SE_UTILS.get_json_bool( obj, key_req_add, dflt_req_add );
            setting.avg_time       = SE_UTILS.get_json_bool( obj, key_avg_time, dflt_avg_time );

            setting.jni_log_level = a_jni_log_level;

            setting.stkdrv_stats_log	= SE_UTILS.get_json_string( obj, dflt_stkdrv_stats_log, dflt_stkdrv_stats_log );
            setting.stkdrv_stats_app	= SE_UTILS.get_json_string( obj, dflt_stkdrv_stats_app, dflt_stkdrv_stats_app );
            setting.stkdrv_stats_res	= SE_UTILS.get_json_string( obj, dflt_stkdrv_stats_res, dflt_stkdrv_stats_res );
        }
        else
        {
            defaults( setting );
        }
    }

}
