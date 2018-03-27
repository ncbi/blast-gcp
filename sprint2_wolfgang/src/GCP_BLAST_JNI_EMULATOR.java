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

import java.io.*;
import java.util.Random;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

class GCP_BLAST_JNI_EMULATOR
{
    public ArrayList< GCP_BLAST_HSP > make_hsp( final GCP_BLAST_JOB job, Integer count, Long oid )
    {
        ArrayList< GCP_BLAST_HSP > res = new ArrayList<>();
        
        for ( int i = 1; i < count; i++ )
            res.add( new GCP_BLAST_HSP( job, oid ) );
            
        return res;
    }
}
