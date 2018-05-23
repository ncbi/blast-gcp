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

public class BLAST_LOG_SETTING implements Serializable
{
    public String host;
    public Integer port;
    public Boolean request;
    public Boolean job_start;
    public Boolean job_done;
    public Boolean cutoff;
    public Boolean log_final;
    public Boolean part_prep;
    public Boolean worker_shift;
    public Boolean pref_loc;
    public Boolean db_copy;
    public String jni_log_level;

    @Override public String toString()
    {
        String S = "\nLOG:\n";
        S = S + String.format( "\tlog_host ........... %s:%d\n", host, port );
        String S_log = "";
        if ( request )   S_log = S_log + "request ";
        if ( job_start ) S_log = S_log + "job_start ";
        if ( job_done )  S_log = S_log + "job_done ";
        if ( cutoff )    S_log = S_log + "cutoff ";
        if ( log_final )     S_log = S_log + "final ";
        if ( part_prep )     S_log = S_log + "part-prep ";
        if ( worker_shift )  S_log = S_log + "worker-shift ";
        if ( pref_loc )  S_log = S_log + "pref_log ";
        if ( db_copy )   S_log = S_log + "db-copy ";

        return S + String.format( "\tlog ................ %s\n", S_log );
    }
}
