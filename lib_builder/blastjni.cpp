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

#include "blastjni.hpp"
#include "gov_nih_nlm_ncbi_blastjni_BLAST_LIB.h"
#include <algo/blast/api/blast4spark.hpp>
#include <algo/blast/api/blast_advprot_options.hpp>
#include <algo/blast/api/blast_exception.hpp>
#include <algo/blast/api/blast_nucl_options.hpp>
#include <algo/blast/api/blast_results.hpp>
#include <algo/blast/api/local_blast.hpp>
#include <algo/blast/api/objmgrfree_query_data.hpp>
#include <algo/blast/api/prelim_stage.hpp>
#include <algo/blast/api/setup_factory.hpp>
#include <algo/blast/core/blast_hspstream.h>
#include <algorithm>
#include <ctype.h>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <jni.h>
#include <ncbi_pch.hpp>
#include <objects/seq/Bioseq.hpp>
#include <objects/seq/Seq_data.hpp>
#include <objects/seqalign/Seq_align.hpp>
#include <objects/seqalign/Seq_align_set.hpp>
#include <objects/seqloc/Seq_id.hpp>
#include <objects/seqset/Bioseq_set.hpp>
#include <objects/seqset/Seq_entry.hpp>
#include <pthread.h>
#include <set>
#include <stdexcept>

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <sstream>
#include <string>
#include <vector>

enum
{
    xc_no_err,
    xc_java_exception,
    xc_blast_exception,

    // should be last
    xc_java_runtime_exception
};

class hsp_stream_cref
{
  private:
    ncbi::blast::TBlastHSPStream * hsp_stream;

  public:
    hsp_stream_cref(void) { hsp_stream = NULL; }
    hsp_stream_cref(ncbi::blast::TBlastHSPStream * h) { hsp_stream = h; }
    ~hsp_stream_cref()
    {
        if (hsp_stream)
            BlastHSPStreamFree(hsp_stream->GetPointer());
        hsp_stream = NULL;
    }
};

/* Utility Functions */
static void jni_throw(JNIEnv * jenv, jclass jexcept_cls, const char * fmt,
                      va_list args)
{
    // expand message into buffer
    char msg[4096];
    int  size = vsnprintf(msg, sizeof msg, fmt, args);

    // ignore overruns
    //
    if (size < 0)
        strcpy(msg, "Error in vsnprintf");
    else if ((size_t)size >= sizeof msg)
        strcpy(&msg[sizeof msg - 4], "...");

    fprintf(stderr, "jni_throwing: %s\n", msg);
    // create error object, put JVM thread into Exception state
    jenv->ThrowNew(jexcept_cls, msg);
}

static void jni_throw(JNIEnv * jenv, uint32_t xtype, const char * fmt,
                      va_list args)
{
    jclass jexcept_cls = 0;

    // select exception types
    switch (xtype)
    {
        case xc_java_exception:
            jexcept_cls = jenv->FindClass("java/lang/Exception");
            break;
    }

    // if not a known type, must throw RuntimeException
    if (!jexcept_cls)
        jexcept_cls = jenv->FindClass("java/lang/RuntimeException");

    jni_throw(jenv, jexcept_cls, fmt, args);
}

static void jni_throw(JNIEnv * jenv, uint32_t xtype, const char * fmt, ...)
{
    if (xtype != xc_no_err)
    {
        va_list args;
        va_start(args, fmt);

        jni_throw(jenv, xtype, fmt, args);

        va_end(args);
    }
}


static void log(JNIEnv * jenv, jobject jthis, jmethodID jlog_method,
                const char * loglevel, const char * fmt, ...)
{
    // struct timespec stime, etime;
    va_list args;
    va_start(args, fmt);

    // Obtain signature via (build.sh makes file 'signatures'):
    //   $ javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_LIB.class

    char buffer[4096];
    int  size = vsnprintf(buffer, sizeof buffer, fmt, args);

    va_end(args);

    if (size < 0)
        strcpy(buffer,
               "log: failed to make a String ( bad format or string "
               "too long )");
    else if ((size_t)size >= sizeof buffer)
        strcpy(buffer, "log: failed to make a String ( string too long )");

    if (jenv->ExceptionCheck())  // Mostly to silence -Xcheck:jni
        fprintf(stderr, "Log method has pending exception\n");

    // make String object
    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &stime);
    jstring jbuffer = jenv->NewStringUTF(buffer);
    if (!jbuffer)
        throw std::runtime_error("Can't create JVM string");

    jstring jloglevel = jenv->NewStringUTF(loglevel);
    if (!jloglevel)
        throw std::runtime_error("Can't create JVM string");

    jenv->CallVoidMethod(jthis, jlog_method, jloglevel, jbuffer);

    if (jenv->ExceptionCheck())  // Mostly to silence -Xcheck:jni
        fprintf(stderr, "Log method has an exception pending.\n");
    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &etime);
    // fprintf(stderr, "1000 iterations took %lu ns\n", etime.tv_nsec -
    // stime.tv_nsec);
    jenv->DeleteLocalRef(jbuffer);
    jenv->DeleteLocalRef(jloglevel);
    return;
}

static jmethodID getlogger(JNIEnv * jenv, jobject jthis)
{
    // Obtain signature via (build.sh makes file 'signatures'):
    //   $ javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_LIB.class
    jclass thiscls = jenv->GetObjectClass(jthis);
    if (!thiscls)
        fprintf(stderr, "couldn't log %p\n", (void *)thiscls);
    jmethodID jlog_method = jenv->GetMethodID(
        thiscls, "log", "(Ljava/lang/String;Ljava/lang/String;)V");
    if (jlog_method)
    {
        log(jenv, jthis, jlog_method, "INFO",
            "Logger method %p, pid=%04d, thread_id=%u", (void *)jlog_method,
            getpid(), pthread_self());
    }
    else
    {
        // FIX - Throw
        fprintf(stderr, "couldn't get methodid %p\n", (void *)jlog_method);
    }

    return jlog_method;
}


/* Prelim_Search Functions */
static jobjectArray iterate_HSPs(JNIEnv * jenv, jobject jthis,
                                 jmethodID                       jlog_method,
                                 std::vector< BlastHSPList * > & hsp_lists,
                                 int                             topn)
{
    log(jenv, jthis, jlog_method, "INFO", "iterate_HSPs has %lu HSP lists",
        hsp_lists.size());

    jclass hsplclass
        = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST");
    if (!hsplclass)
        throw std::runtime_error("Can't get hspl class");


    jmethodID hspl_ctor_id = jenv->GetMethodID(hsplclass, "<init>", "(II[B)V");
    if (!hspl_ctor_id)
        throw std::runtime_error("Can't find hspl ctor method");


    /*  But first, an explanation of what we have, and what we need to return:

        prelim-search has returned to us a "stream" ( iterator upon a list ) of
        hsp_list objects, which are tuples. Conceptually, they are intended to
        be

        ( query, subject, max-score, { hsps } )

        So the stream is really a sequence of these tuples, i.e.

        < ( q, s, max, { hsps } ), ( q, s, max, { hsps }, ... >

        The actual hsp_list is not quite like this. The query has no
        representation because it is common across all hsp_lists, and so is
        factored out. The subject is not represented by value, but by the pair
        (
        db-spec, oid ), yielding
        ( implicit-query, ( db-spec, oid ), max-score, { hsps } )

        The db-spec is again not represented, but factored out since it is
        common.  The max-score is implied as a function on the set of hsps,
        giving
        ( implicit-query, ( implicit-db-spec, oid ), implicit-max-score, {
        hsps
        } )

        What we need to return is again a sequence of tuples, but our tuples
        should look like this:
        ( implicit-query, implicit-db-spec, oid, max-score, { hsps } )

        where the main difference is that we make max-score explicit, since it
        is the basis for the reduce phase. We regroup the implicit members into
        a single tuple called job:

        job = ( query, db-spec, ... )

        giving us a ( hopefully ) final tuple of

        ( job, oid, max-score, { hsps } )

        Note that you added some timing information, which is great, but it has
        to be associated with the job and not with each hsp_list tuple.

        The HSPs that are given as sets above, should NOT include oid, unless
        we're producing a flat table ( and we're not ). Furthermore, we can
        observe that they can remain opaque to Java because we're only holding
        onto them long enough to pass them back into the traceback.

        So we need to return a sequence:

        < ( job, oid, max-score, { hsps } ), ( job, oid, max-score, { hsps
        }
        ),
        ... >

        Technically, we are returning a set of tuples, because there is no
        implied ordering of them, and each entry is required to be unique by at
        least oid.

        { ( job, oid1, max-score, { hsps } ), ( job, oid2, max-score, {
        hsps
        }
        ), ... }

        We know that the purpose of the reduce stage is to
        1) merge all sequences, then
        2) select top-N tuples. Knowing N now gives the opportunity to apply
        some filtering to our sequence/set of tuples so that we never return
        more than N tuples.

        NB - the top-N does NOT apply to the inner set of hsps, which are
    allowed to exceed N.  */

    /*  PSEUDO/SKELETON CODE
        given:
        job       : an externally created tuple of implied members
        hsp_lists : a sequence of hsp_list*
        topn         : the maximum number of tuples to return

        declare:
        max_scores: a sequence of int corresponding by ordinal to the
        hsp_lists
        score_set : a set of max-scores observed
        num-tuples: a count of the number of tuples to return in
        sequence/set
        min-score : a cutoff score for our own top-N filtering
        tuples    : an array to act as sequence for our tuples
        */

    std::vector< int > max_scores;
    std::set< int >    score_set;
    size_t             num_tuples = 0;
    int                min_score  = INT_MIN;

    jclass bclass = jenv->FindClass("[B");
    if (!bclass)
        throw std::runtime_error("can't create byte array");

    /*     begin
    // determine the max scores
    for i in 0 .. hsp_lists . size
    {
    declare hsp_list := hsp_lists [ i ]
    declare max-score := MIN_INT;
    for each hsp in hsp_list . hsp_array
    if max-score < hsp . score
    max-score := hsp . score
    max_scores [ i ] := max-score
    score_set . add ( max-score )
    }
    */
    max_scores.reserve(hsp_lists.size());
    for (size_t i = 0; i != hsp_lists.size(); ++i)
    {
        const BlastHSPList * hsp_list = hsp_lists[i];
        log(jenv, jthis, jlog_method, "DEBUG", "  HSP list #%d, oid=0x%x", i,
            hsp_list->oid);
        int max_score = INT_MIN;

        if (hsp_list->hspcnt)
        {
            for (int h = 0; h != hsp_list->hspcnt; ++h)
            {
                int hsp_score = hsp_list->hsp_array[h]->score;
                log(jenv, jthis, jlog_method, "DEBUG",
                    "      HSP #%d hsp_score=%d (0x%x)", h, hsp_score,
                    hsp_score);
                if (max_score < hsp_score)
                    max_score = hsp_score;
            }
        }
        else
        {
            log(jenv, jthis, jlog_method, "INFO", "iterate_HSPs, zero hspcnt");
        }

        max_scores.push_back(max_score);
        score_set.insert(max_score);

        log(jenv, jthis, jlog_method, "DEBUG", "  have %lu max_scores",
            max_scores.size());
        log(jenv, jthis, jlog_method, "DEBUG", "  have %lu in score_set",
            score_set.size());
    }

    /*  assume for the moment that all tuples will make cut
        num-tuples := hsp_lists . size

    // determine minimum cutoff on max-score
    min-score := MIN_INT;
    if |score_set| > N
    {
    min-score := min ( top-N ( score_set ) )

    // count how many tuples are going to make the cut
    num-tuples := 0
    for each score in max_scores
    if score >= min-score
    num-tuples := num-tuples + 1
    }
    */

    if ((int)score_set.size() > topn)
    {
        int top = topn;
        for (auto sit = score_set.rbegin(); sit != score_set.rend(); ++sit)
        {
            --top;
            if (!top)
            {
                min_score = *sit;
                break;
            }
        }

        for (size_t i = 0; i != max_scores.size(); ++i)
            if (max_scores[i] >= min_score)
                ++num_tuples;
    }
    else
        num_tuples = hsp_lists.size();

    log(jenv, jthis, jlog_method, "DEBUG", "  min_score is %d", min_score);
    log(jenv, jthis, jlog_method, "DEBUG", "  num_tuples is %lu", num_tuples);

    /*   allocate return array/sequence/set
tuples := JVM . allocateMeAnObjectArray ( num-tuples )
*/
    jobjectArray retarray = jenv->NewObjectArray(num_tuples, hsplclass, NULL);

    /*
       build the sequence
       declare j := 0
       for i in 0 .. hsp_lists . size
       {
       */
    size_t j = 0;
    for (size_t i = 0; i != hsp_lists.size(); ++i)
    {
        if (max_scores[i] >= min_score)
        {
            log(jenv, jthis, jlog_method, "DEBUG",
                "  adding hsp_list[%d]: max_scores[%d]=%d >= min_score of %d",
                i, i, max_scores[i], min_score);
            /*
               apply filtering
               if max_scores [ i ] >= min-score
               declare hsp_list := hsp_lists [ i ];

               we now have enough information to have the JVM allocate
               our tuple. It is proper to include the oid in this tuple,
               because it is supposed to have an unique constraint on it
               as a set key, and will be useful for logging. But otherwise,
               our Java code won't need it.

               "**" if we are using an integer packing format for the
               HSPs,  the work would need to be done up front so we know the
               */
            const BlastHSPList * hsp_list = hsp_lists[i];
            size_t blob_size = hsp_list->hspcnt * sizeof(ncbi::blast::SFlatHSP);

            ncbi::blast::SFlatHSP * hspblob
                = (ncbi::blast::SFlatHSP *)malloc(blob_size);
            if (!hspblob)
                throw std::runtime_error("Couldn't allocate hspblob");

            try
            {
                /*
                   declare tuple := JVM . allocateTuple ( job, hsp_list . oid,
                   max_scores [ i ], hsp_list . num_hsps * HSP_STRUCT_SIZE
                   )

                   the JVM will have allocated memory for us in the form of
                   a byte[] used as a blob for the { hsps }
                   */
                log(jenv, jthis, jlog_method, "DEBUG", "  blob #%d size is %lu",
                    i, blob_size);
                jbyteArray tuple = jenv->NewByteArray(blob_size);
                if (!tuple)
                    throw std::runtime_error("Couldn't create ByteArray");

                /*
                   declare hsp_blob: a sequence of HSP tuples, each with
                   size
                   HSP_STRUCT_SIZE
hsp_blob := tuple . hsp_blob

we now record the HSP data within the blob
                // NB - this is NOT subject to top-N
                for each hsp in hsp_list . hsp_array
                // NB - this might eventually be accomplished with
                integer
                compression  by using a packing format. The size would
                need to
                be known well above, where I place "**"

                append hsp to hsp_blob
                */
                size_t idx = 0;
                int    oid = hsp_list->oid;
                for (int h = 0; h != hsp_list->hspcnt; ++h)
                {
                    // FIX - don't need/want oid in the wire protocol
                    // Passed to traceback as argument, returned?
                    hspblob[idx].oid   = oid;
                    hspblob[idx].score = hsp_list->hsp_array[h]->score;
                    hspblob[idx].query_start
                        = hsp_list->hsp_array[h]->query.offset;
                    hspblob[idx].query_end = hsp_list->hsp_array[h]->query.end;
                    hspblob[idx].query_frame
                        = hsp_list->hsp_array[h]->query.frame;
                    hspblob[idx].query_gapped_start
                        = hsp_list->hsp_array[h]->query.gapped_start;
                    hspblob[idx].subject_start
                        = hsp_list->hsp_array[h]->subject.offset;
                    hspblob[idx].subject_end
                        = hsp_list->hsp_array[h]->subject.end;
                    hspblob[idx].subject_frame
                        = hsp_list->hsp_array[h]->subject.frame;
                    hspblob[idx].subject_gapped_start
                        = hsp_list->hsp_array[h]->subject.gapped_start;

                    ++idx;
                }

                // Copy hspblob into Java tuple
                jenv->SetByteArrayRegion(tuple, 0, blob_size,
                                         (const jbyte *)hspblob);
                // Create a new HSP_LIST
                jobject hspl_obj = jenv->NewObject(hsplclass, hspl_ctor_id, oid,
                                                   max_scores[i], tuple);
                if (!hspl_obj)
                {
                    log(jenv, jthis, jlog_method, "ERROR",
                        "Couldn't make new HSP_LIST");
                    throw std::runtime_error("Couldn't make new HSP_LIST");
                }

                jenv->DeleteLocalRef(tuple);
                /* done with this tuple
                 * tuples [ j ] := tuple
                 * j := j + 1
                 */
                if (j > num_tuples)
                    log(jenv, jthis, jlog_method, "ERROR", "array overflow");
                log(jenv, jthis, jlog_method, "DEBUG",
                    "ps Setting Java Object Array Element %d", j);
                jenv->SetObjectArrayElement(retarray, j, hspl_obj);
                jenv->DeleteLocalRef(hspl_obj);
                log(jenv, jthis, jlog_method, "DEBUG",
                    "ps Set     Java Object Array Element %d", j);
                ++j;
            }
            catch (...)
            {
                log(jenv, jthis, jlog_method, "ERROR",
                    "exception in iterate_HSPs");
                free(hspblob);
                hspblob = NULL;
                throw;
            }

            free(hspblob);
            hspblob = NULL;
        }
        else
        {
            log(jenv, jthis, jlog_method, "DEBUG",
                "  skipping hsp_list[%d]: max_scores[%d]=%d < min_score of %d",
                i, i, max_scores[i], min_score);
        }
    }
    log(jenv, jthis, jlog_method, "DEBUG", "  iterate_HSPs done");
    // Return tuples to Java object
    // done
    return retarray;

    /*
       The pseudo code above has the properties of only allocating memory in the
       JVM, and only when necessary, and only in the amounts necessary. It also
       has the property of creating the correct units for reduce.

       The same procedure would be applied for the results of traceback, except
       of course that you would use bottom-N evalues instead of top-N scores. It
       almost begs for abstracting the "max-scores" array and "score-set" so
       that
       they do the right thing.
       */
}

static void whack_hsp_lists(std::vector< BlastHSPList * > & hsp_lists)
{
    size_t i, count = hsp_lists.size();
    for (i = 0; i < count; ++i)
        Blast_HSPListFree(hsp_lists[i]);
}

static jobjectArray prelim_search(JNIEnv * jenv, jobject jthis,
                                  jmethodID jlog_method, const char * jquery,
                                  const char * jdb_spec, const char * jprogram,
                                  const char * jparams, jint topn)
{
    // if ( jenv->EnsureLocalCapacity( 1024 ) )
    //    throw std::runtime_error( "Can't ensure local capacity" );

    log(jenv, jthis, jlog_method, "DEBUG",
        "Blast prelim_search called with\n"
        "  query   : %s\n"
        "  db_spec : %s\n"
        "  program : %s\n"
        "  topn    : %d\n",
        jquery, jdb_spec, jprogram, topn);
    //      "  params  : %s"

    ncbi::blast::TBlastHSPStream * hsp_stream = ncbi::blast::PrelimSearch(
        std::string(jquery), std::string(jdb_spec), std::string(jprogram),
        std::string(jparams));

    log(jenv, jthis, jlog_method, "INFO", "Blast prelim_search returned");

    if (!hsp_stream)
    {
        log(jenv, jthis, jlog_method, "ERROR", "NULL hsp_stream");
        throw std::runtime_error("prelim_search - NULL hsp_stream");
    }

    hsp_stream_cref cref_hsp(hsp_stream);

    std::vector< BlastHSPList * > hsp_lists;
    try
    {
        log(jenv, jthis, jlog_method, "DEBUG", "Begin BlastHSPStreamRead loop");
        while (1)
        {
            BlastHSPList * hsp_list = 0;
            int            status
                = BlastHSPStreamRead(hsp_stream->GetPointer(),
                                     &hsp_list);  // FIX, use operator ->?

            if (status == kBlastHSPStream_Error)
            {
                log(jenv, jthis, jlog_method, "ERROR", "kBlastHSPStream_Error");
                throw std::runtime_error(
                    "prelim_search - Exception from BlastHSPStreamRead");
            }

            if (status != kBlastHSPStream_Success || !hsp_list)
                break;

            if (hsp_list->oid != -1)
                hsp_lists.push_back(hsp_list);
            else
                log(jenv, jthis, jlog_method, "WARN", "skipping oid==-1");
        }

        log(jenv, jthis, jlog_method, "DEBUG", "  loop complete");
    }
    catch (...)
    {
        whack_hsp_lists(hsp_lists);
        log(jenv, jthis, jlog_method, "ERROR", "exception in loop");
        throw;
    }
    jobjectArray ret = iterate_HSPs(jenv, jthis, jlog_method, hsp_lists, topn);

    whack_hsp_lists(hsp_lists);

    return ret;
}

/*
 * Class:     gov_nih_nlm_ncbi_blastjni_BLAST_LIB
 * Method:    prelim_search
 * Signature:
 * (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILgov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST;)Z
 */
JNIEXPORT jobjectArray JNICALL
                       Java_gov_nih_nlm_ncbi_blastjni_BLAST_1LIB_prelim_1search(
    JNIEnv * jenv, jobject jthis, jstring jquery, jstring jdb_spec,
    jstring jprogram, jstring jparams, jint topn)
{
    uint32_t xtype = xc_no_err;

    const char * query       = jenv->GetStringUTFChars(jquery, 0);
    const char * db_spec     = jenv->GetStringUTFChars(jdb_spec, 0);
    const char * program     = jenv->GetStringUTFChars(jprogram, 0);
    const char * params      = jenv->GetStringUTFChars(jparams, 0);
    jmethodID    jlog_method = getlogger(jenv, jthis);

    log(jenv, jthis, jlog_method, "INFO", "C++ jni_prelim_1search called with");
    log(jenv, jthis, jlog_method, "INFO", "  query   : %s", query);
    log(jenv, jthis, jlog_method, "INFO", "  db_spec : %s", db_spec);
    log(jenv, jthis, jlog_method, "INFO", "  program : %s", program);
    log(jenv, jthis, jlog_method, "INFO", "  params  : %s", params);
    log(jenv, jthis, jlog_method, "INFO", "  topn    : %d", topn);
    jobjectArray ret = NULL;
    try
    {
        ret = prelim_search(jenv, jthis, jlog_method, query, db_spec, program,
                            params, topn);
    }
    catch (std::exception & x)
    {
        jenv->ReleaseStringUTFChars(jquery, query);
        jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
        jenv->ReleaseStringUTFChars(jprogram, program);
        jenv->ReleaseStringUTFChars(jparams, params);
        jni_throw(jenv, xtype = xc_java_exception, "%s", x.what());
    }
    // FIX -
    // https://ncbiconfluence.ncbi.nlm.nih.gov/pages/viewpage.action?spaceKey=BLASTGCP&title=Errors+reported+by+BLAST
    /*
       catch (ncbi::blast::CInputException& x) {
       jni_throw(jenv, xtype = xc_blast_exception, "%s", x.GetMsg().data());
       }
       catch (CException& x) {
       jni_throw(jenv, xtype = xc_blast_exception, "%s", x.GetMsg().data());
       }
       */
    catch (...)
    {
        jenv->ReleaseStringUTFChars(jquery, query);
        jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
        jenv->ReleaseStringUTFChars(jprogram, program);
        jenv->ReleaseStringUTFChars(jparams, params);
        jni_throw(jenv, xtype = xc_java_runtime_exception,
                  "%s - unknown exception", __func__);
    }

    log(jenv, jthis, jlog_method, "INFO", "C++ prelim_search done");
    jenv->ReleaseStringUTFChars(jquery, query);
    jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
    jenv->ReleaseStringUTFChars(jprogram, program);
    jenv->ReleaseStringUTFChars(jparams, params);
    return ret;
}

/* Traceback Functions */
static jobjectArray traceback(JNIEnv * jenv, jobject jthis,
                              jmethodID jlog_method, const char * jquery,
                              const char * jdb_spec, const char * jprogram,
                              const char * jparams, jobjectArray hspl_obj)
{
    // if ( jenv->EnsureLocalCapacity( 1024 ) )
    //    throw std::runtime_error( "Can't ensure local capacity" );

    log(jenv, jthis, jlog_method, "INFO", "Blast traceback called with");
    log(jenv, jthis, jlog_method, "INFO", "  query    : %s", jquery);
    log(jenv, jthis, jlog_method, "INFO", "  db_spec  : %s", jdb_spec);
    log(jenv, jthis, jlog_method, "INFO", "  program  : %s", jprogram);

    jsize hspl_sz = jenv->GetArrayLength(hspl_obj);
    log(jenv, jthis, jlog_method, "INFO", "  hsp_lists: %d", hspl_sz);

    jclass hsplclass
        = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST");
    if (!hsplclass)
        throw std::runtime_error("Can't get hspl class");

    jfieldID hspl_blob_fid = jenv->GetFieldID(hsplclass, "hsp_blob", "[B");
    if (!hspl_blob_fid)
        throw std::runtime_error("Can't get hsp_blob fieldID");

    jint                                 oid = -1;
    std::vector< ncbi::blast::SFlatHSP > flat_hsp_list;
    flat_hsp_list.reserve(hspl_sz * 4 / 3);
    // Iterate through HSP_LIST[]
    for (int h = 0; h != hspl_sz; ++h)
    {
        jobject hspl = jenv->GetObjectArrayElement(hspl_obj, h);
        if (!hspl)
            throw std::runtime_error("Couldn't get array element");

        jobject blobobj = jenv->GetObjectField(hspl, hspl_blob_fid);
        if (!blobobj)
            throw std::runtime_error("Couldn't get blob array");
        jbyteArray blobarr = (jbyteArray)blobobj;

        size_t blob_size = jenv->GetArrayLength(blobarr);

        jbyte * be = jenv->GetByteArrayElements(blobarr, NULL);
        if (be == NULL)
            throw std::runtime_error("Couldn't get bytearray");
        ncbi::blast::SFlatHSP * flathsps = (ncbi::blast::SFlatHSP *)be;

        size_t elements = blob_size / sizeof(ncbi::blast::SFlatHSP);
        log(jenv, jthis, jlog_method, "DEBUG",
            "  blob will have %lu elements: %lu bytes", elements, blob_size);

        for (size_t i = 0; i != elements; ++i)
        {
            ncbi::blast::SFlatHSP flathsp = flathsps[i];
            flat_hsp_list.push_back(flathsp);
        }
        jenv->ReleaseByteArrayElements(blobarr, be,
                                       JNI_ABORT);  // Didn't update the array
        jenv->DeleteLocalRef(hspl);
        jenv->DeleteLocalRef(blobobj);
    }

    ncbi::blast::TIntermediateAlignments alignments;
    int                                  result = ncbi::blast::TracebackSearch(
        std::string(jquery), std::string(jdb_spec), std::string(jprogram),
        std::string(jparams), flat_hsp_list, alignments);
    size_t num_alignments = alignments.size();
    log(jenv, jthis, jlog_method, "INFO",
        "Blast traceback returned status=%d. Got %d alignments", result,
        num_alignments);

    // Get class for TB_LIST
    jclass tbcls = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST");
    if (!tbcls)
        throw std::runtime_error("Can't get tb class");

    jmethodID tb_ctor_id = jenv->GetMethodID(tbcls, "<init>", "(ID[B)V");
    if (!tb_ctor_id)
        throw std::runtime_error("Can't find tb ctor method");

    jobjectArray retarray = jenv->NewObjectArray(num_alignments, tbcls, NULL);
    for (size_t i = 0; i != num_alignments; ++i)
    {
        jdouble     evalue = alignments[i].first;
        std::string asn    = alignments[i].second;
        oid                = flat_hsp_list[i].oid;

        log(jenv, jthis, jlog_method, "DEBUG",
            "  evalue=%f, oid=%d, ASN is %lu bytes", evalue, oid, asn.size());

        jbyteArray asn_blob = jenv->NewByteArray(asn.size());
        if (!asn_blob)
            throw std::runtime_error("Can't make bytearray");
        jenv->SetByteArrayRegion(asn_blob, 0, asn.size(),
                                 (const jbyte *)asn.data());
        jobject tb_obj
            = jenv->NewObject(tbcls, tb_ctor_id, oid, evalue, asn_blob);
        if (!tb_obj)
            throw std::runtime_error("Couldn't make new TB_LIST");

        log(jenv, jthis, jlog_method, "DEBUG",
            "tb Setting Java Object Array Element %d", i);
        jenv->SetObjectArrayElement(retarray, i, tb_obj);
        jenv->DeleteLocalRef(asn_blob);
        jenv->DeleteLocalRef(tb_obj);
        log(jenv, jthis, jlog_method, "DEBUG",
            "tb Set     Java Object Array Element %d", i);
    }

    return retarray;
}

/*
 * Class:     gov_nih_nlm_ncbi_blastjni_BLAST_LIB
 * Method:    traceback
 * Signature:
 * (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Lgov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST;Lgov/nih/nlm/ncbi/blastjni/BLAST_JOB;)[Lgov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST;
 */
JNIEXPORT jobjectArray JNICALL
                       Java_gov_nih_nlm_ncbi_blastjni_BLAST_1LIB_traceback(
    JNIEnv * jenv, jobject jthis, jobjectArray hspl, jstring jquery,
    jstring jdb_spec, jstring jprogram, jstring jparams)
{
    jmethodID jlog_method = getlogger(jenv, jthis);
    uint32_t  xtype       = xc_no_err;

    const char * query   = jenv->GetStringUTFChars(jquery, 0);
    const char * db_spec = jenv->GetStringUTFChars(jdb_spec, 0);
    const char * program = jenv->GetStringUTFChars(jprogram, 0);
    const char * params  = jenv->GetStringUTFChars(jparams, 0);

    log(jenv, jthis, jlog_method, "DEBUG", "C++ jni_traceback called with");
    log(jenv, jthis, jlog_method, "DEBUG", "  query   : %s", query);
    log(jenv, jthis, jlog_method, "DEBUG", "  db_spec : %s", db_spec);
    log(jenv, jthis, jlog_method, "DEBUG", "  program : %s", program);
    //    log(jenv, jthis, jlog_method, "  blob    : %lu bytes", blob_size);
    jobjectArray ret = NULL;
    try
    {
        ret = traceback(jenv, jthis, jlog_method, query, db_spec, program,
                        params, hspl);
    }
    catch (std::exception & x)
    {
        jni_throw(jenv, xtype = xc_java_exception, "%s", x.what());
    }
    catch (...)
    {
        jni_throw(jenv, xtype = xc_java_runtime_exception,
                  "%s - unknown exception", __func__);
    }

    log(jenv, jthis, jlog_method, "INFO", "C++ traceback done");
    jenv->ReleaseStringUTFChars(jquery, query);
    jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
    jenv->ReleaseStringUTFChars(jprogram, program);
    jenv->ReleaseStringUTFChars(jparams, params);
    return ret;
}

static std::vector< ncbi::blast::SFlatHSP >
iterate_HSPs_nojni(std::vector< BlastHSPList * > & hsp_lists, int topn)
{
    std::vector< int > max_scores;
    std::set< int >    score_set;
    size_t             num_tuples = 0;
    int                min_score  = INT_MIN;

    for (const auto & hsp_list : hsp_lists)
    {
        int max_score = INT_MIN;

        if (hsp_list->hspcnt)
        {
            for (int h = 0; h != hsp_list->hspcnt; ++h)
            {
                int hsp_score = hsp_list->hsp_array[h]->score;
                if (max_score < hsp_score)
                    max_score = hsp_score;
            }
        }

        max_scores.push_back(max_score);
        score_set.insert(max_score);
    }

    fprintf(stderr, "score_set has %zu\n", score_set.size());

    if ((int)score_set.size() > topn)
    {
        int top = topn;
        for (auto sit = score_set.rbegin(); sit != score_set.rend(); ++sit)
        {
            --top;
            if (!top)
            {
                min_score = *sit;
                break;
            }
        }

        for (const auto max_score : max_scores)
            if (max_score >= min_score)
                ++num_tuples;
    }
    else
        num_tuples = hsp_lists.size();

    fprintf(stderr, "num_tuples is %zu, min_score is %d \n", num_tuples,
            min_score);

    std::vector< ncbi::blast::SFlatHSP > retarray;  // (num_tuples);

    for (size_t i = 0; i != hsp_lists.size(); ++i)
    {
        if (max_scores[i] >= min_score)
        {
            const BlastHSPList * hsp_list = hsp_lists[i];

            int oid = hsp_list->oid;
            for (int h = 0; h != hsp_list->hspcnt; ++h)
            {
                struct ncbi::blast::SFlatHSP flathsp;

                flathsp.oid         = oid;
                flathsp.score       = hsp_list->hsp_array[h]->score;
                flathsp.query_start = hsp_list->hsp_array[h]->query.offset;
                flathsp.query_end   = hsp_list->hsp_array[h]->query.end;
                flathsp.query_frame = hsp_list->hsp_array[h]->query.frame;
                flathsp.query_gapped_start
                    = hsp_list->hsp_array[h]->query.gapped_start;
                flathsp.subject_start = hsp_list->hsp_array[h]->subject.offset;
                flathsp.subject_end   = hsp_list->hsp_array[h]->subject.end;
                flathsp.subject_frame = hsp_list->hsp_array[h]->subject.frame;
                flathsp.subject_gapped_start
                    = hsp_list->hsp_array[h]->subject.gapped_start;

                retarray.push_back(flathsp);
            }
        }
    }
    return retarray;
}


// typedef std::vector<std::pair<double, std::string>> TIntermediateAlignments
ncbi::blast::TIntermediateAlignments
searchandtb(std::string query, std::string db_spec, std::string program,
            std::string params, int top_n_prelim, int top_n_traceback)
{
    fprintf(stderr, "Calling PrelimSearch\n");
    ncbi::blast::TBlastHSPStream * hsp_stream
        = ncbi::blast::PrelimSearch(query, db_spec, program, params);
    fprintf(stderr, "Called  PrelimSearch\n");

    if (!hsp_stream)
    {
        fprintf(stderr, "NULL hsp_stream");
    }

    std::vector< BlastHSPList * > hsp_lists;
    hsp_stream_cref               cref_hsp(hsp_stream);

    try
    {
        while (1)
        {
            BlastHSPList * hsp_list = NULL;
            int            status
                = BlastHSPStreamRead(hsp_stream->GetPointer(), &hsp_list);

            if (status == kBlastHSPStream_Error)
            {
                fprintf(stderr, "ERROR kBlastHSPStream_Error");
                break;
            }

            if (status != kBlastHSPStream_Success || !hsp_list)
            {
                break;
            }

            if (hsp_list->oid != -1)
                hsp_lists.push_back(hsp_list);
        }
    }
    catch (...)
    {
        fprintf(stderr, "ERROR exception in loop");
    }

    fprintf(stderr, "Have %zu hsp_lists\n", hsp_lists.size());

    std::vector< ncbi::blast::SFlatHSP > flat_hsp_list
        = iterate_HSPs_nojni(hsp_lists, top_n_prelim);

    whack_hsp_lists(hsp_lists);

    for (size_t i = 0; i != flat_hsp_list.size(); ++i)
    {
        fprintf(stderr, "Flat HSP #%zu ", i);
        fprintf(stderr, "oid=%d ", flat_hsp_list[i].oid);
        fprintf(stderr, "score=%d ", flat_hsp_list[i].score);
        fprintf(stderr, "\n");
    }

    ncbi::blast::TIntermediateAlignments alignments;

    fprintf(stderr, "Ignoring top_n_traceback=%d\n", top_n_traceback);

    // FIX: top_n_traceback

    fprintf(stderr, "Calling TracebackSearch with %zu flat HSPs\n",
            flat_hsp_list.size());
    int result = ncbi::blast::TracebackSearch(query, db_spec, program, params,
                                              flat_hsp_list, alignments);
    fprintf(stderr,
            "Called  TracebackSearch, returned %d, got %zu alignments\n",
            result, alignments.size());


    return alignments;
}
