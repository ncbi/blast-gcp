/* ===========================================================================
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

#include "gov_nih_nlm_ncbi_blastjni_BlastJNI.h"
#include <jni.h>
#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>

static void log(const char* msg)
{
    const bool debug = true;

    if (debug) {
        char fname[64];
        sprintf(fname, "/tmp/blastjni.%u.log", getpid());
        FILE* fout = fopen(fname, "a");
        if (!fout) return;
        fputs(msg, fout);
        fputc('\n', fout);
        fclose(fout);
    }
}

static unsigned long long fakerng(unsigned long mod)
{
    static unsigned long long state = 1;

    state *= 6364136223846793005ULL;
    state += 1442695040888963407ULL;

    return state % mod;
}

// JNIEXPORT jobjectArray JNICALL
JNIEXPORT jobjectArray JNICALL
Java_BlastJNI_prelim_1search(
    JNIEnv* env, jobject jobj, jstring jobid, jstring query, jstring db,
    jstring params)
{
    log("Entered Java_BlastJNI_prelim_1search");

    const char* cjobid = env->GetStringUTFChars(jobid, NULL);
    log(cjobid);

    const char* cquery = env->GetStringUTFChars(query, NULL);
    log(cquery);

    const char* cdb = env->GetStringUTFChars(db, NULL);
    log(cdb);

    const char* cparams = env->GetStringUTFChars(params, NULL);
    log(cparams);

    std::string cppquery;
    // cppquery = "CHUNKGOESHERE";
    // cppquery.append(cquery);

    // BlastHSPStream* PrelimSearch(
    //      const std::string& single_query,
    //      const std::string& database_name,
    //      const std::string& program_name);
    /*
        BlastHSPStream's sorted_hsplists->
        BlastHSPList's hsplist_array ->
        BlashHSPList's hsp_array
        BlastHSP hsp_array[] (BlastHSP bit_score and evalue are double)
           or
        BlastHSPStream's results->
        BlastHSPResults's hislist_array ->
        BlastHitList's hsplist_array ->
        BlastHSPList's hsplist_array[] (worst_evalue is double, low_score is
       integer)
Use:

       BlastHSPStreamRead(BlastHSPStream* hsp_stream, BlastHSPList**
hsp_list_out
    */

    size_t numelems = fakerng(20);
    jobjectArray ret;
    ret = (jobjectArray)env->NewObjectArray(
        numelems, env->FindClass("java/lang/String"), NULL);

    long job_id = 1;
    long chunk_id = fakerng(200);
    for (size_t i = 0; i != numelems; ++i) {
        char buf[256];
        long oid = fakerng(3000000);
        long score = fakerng(1000); // TODO: Could be double
        long qstart = fakerng(500);
        long qstop = qstart + fakerng(100);
        long sstart = fakerng(4 * 4294967296);
        long sstop = sstart + fakerng(300);
        // HSP: Job-ID, Chunk, OID, High Score, start/stops, score
        // TODO: Populate HSP Java object rather than String
        sprintf(buf,
                "{ \"chunk\": %lu, \"jobid\": %lu, \"oid\": %lu, \"score\": "
                "%lu, \"qstart\": %lu, \"qstop\": %lu, \"sstart\": %lu, "
                "\"sstop\": %lu }\n",
                chunk_id, job_id, oid, score, qstart, qstop, sstart, sstop);

        //        sprintf(buf, "%s,%s,%lu,%lu,%lu,%lu,%lu,%lu\n",
        //        cppquery.data(),
        //               cjobid, oid, score, qstart, qstop, sstart, sstop);
        env->SetObjectArrayElement(ret, i, env->NewStringUTF(buf));
    }

    env->ReleaseStringUTFChars(jobid, cjobid);
    env->ReleaseStringUTFChars(query, cquery);
    env->ReleaseStringUTFChars(db, cdb);
    env->ReleaseStringUTFChars(params, cparams);
    log("Leaving Java_BlastJNI_prelim_1search");
    return (ret);

    // TODO: Exceptions: env->Throw(...)
}
