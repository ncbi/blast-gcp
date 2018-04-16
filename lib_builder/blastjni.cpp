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
#include <set>
#include <stdexcept>

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>
#include <string>
#include <vector>

/* Utility Functions */

// FIX: Replace with proper enum or Log4J Level's
enum LOG_LEVEL
{
    LOG_TRACE,
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_FATAL
};

enum
{
    xc_no_err,
    xc_java_exception,
    xc_blast_exception,

    // should be last
    xc_java_runtime_exception
};

static void jni_throw(JNIEnv* jenv, jclass jexcept_cls, const char* fmt, va_list args)
{
    // expand message into buffer
    char msg[4096];
    int size = vsnprintf(msg, sizeof msg, fmt, args);

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

static void jni_throw(JNIEnv* jenv, uint32_t xtype, const char* fmt, va_list args)
{
    jclass jexcept_cls = 0;

    // select exception types
    switch (xtype) {
        case xc_java_exception:
            jexcept_cls = jenv->FindClass("java/lang/Exception");
            break;
    }

    // if not a known type, must throw RuntimeException
    if (!jexcept_cls)
        jexcept_cls = jenv->FindClass("java/lang/RuntimeException");

    jni_throw(jenv, jexcept_cls, fmt, args);
}

static void jni_throw(JNIEnv* jenv, uint32_t xtype, const char* fmt, ...)
{
    if (xtype != xc_no_err) {
        va_list args;
        va_start(args, fmt);

        jni_throw(jenv, xtype, fmt, args);

        va_end(args);
    }
}

static void log(JNIEnv* jenv, jobject jthis, LOG_LEVEL level, const char* fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    // Obtain signature via (build.sh makes file 'signatures'):
    //   $ javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_LIB.class

    char buffer[4096];
    int size = vsnprintf(buffer, sizeof buffer, fmt, args);

    va_end(args);

    if (size < 0)
        strcpy(buffer,
               "log: failed to make a String ( bad format or string "
               "too long )");
    else if ((size_t)size >= sizeof buffer)
        strcpy(buffer, "log: failed to make a String ( string too long )");

    // make String object, JVM will garbage collect the jstring
    jstring jstr = jenv->NewStringUTF(buffer);
    if (!jstr)
        throw std::runtime_error("Can't create JVM string");

    jclass thiscls = jenv->GetObjectClass(jthis);
    if (!thiscls)
        throw std::runtime_error("Can't get this class");

    const char* func = "";
    switch (level) {
        case LOG_TRACE:
            func = "log_trace";
            break;
        case LOG_DEBUG:
            func = "log_debug";
            break;
        case LOG_INFO:
            func = "log_info";
            break;
        case LOG_WARN:
            func = "log_warn";
            break;
        case LOG_ERROR:
            func = "log_error";
            break;
        case LOG_FATAL:
            func = "log_fatal";
            break;
        default:
            func = "log_fatal";
            break;
    }

    jmethodID jlog_method = jenv->GetMethodID(thiscls, func, "(Ljava/lang/String;)V");
    if (!jlog_method)
        throw std::runtime_error("Couldn't get log function");

    jenv->CallVoidMethod(jthis, jlog_method, jstr);
    if (jenv->ExceptionCheck())  // Mostly to silence -Xcheck:jni
        fprintf(stderr, "Log method %s threw an exception, which it should never do.\n", func);
    return;
}


/* Prelim_Search Functions */
static jobjectArray iterate_HSPs(JNIEnv* jenv, jobject jthis, std::vector<BlastHSPList*>& hsp_lists,
                                 int topn)
{
    log(jenv, jthis, LOG_INFO, "iterate_HSPs has %lu HSP lists:", hsp_lists.size());

    jclass hsplclass = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST");
    if (!hsplclass)
        throw std::runtime_error("Can't get hspl class");


    jmethodID hspl_ctor_id = jenv->GetMethodID(hsplclass, "<init>", "(II[B)V");
    if (!hspl_ctor_id)
        throw std::runtime_error("Can't find hspl ctor method");


    /*  But first, an explanation of what we have, and what we need to return:

        prelim-search has returned to us a "stream" ( iterator upon a list ) of
        hsp_list objects, which are tuples. Conceptually, they are intended to be

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
        common.
        The max-score is implied as a function on the set of hsps, giving
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
    std::vector<int> max_scores;
    std::set<int> score_set;
    size_t num_tuples = 0;
    int min_score = INT_MIN;

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
    for (size_t i = 0; i != hsp_lists.size(); ++i) {
        const BlastHSPList* hsp_list = hsp_lists[i];
        log(jenv, jthis, LOG_DEBUG, "  HSP list #%d, oid=0x%x", i, hsp_list->oid);
        if (!hsp_list->hspcnt) {
            log(jenv, jthis, LOG_DEBUG, "iterate_HSPs, zero hspcnt");
            continue;
        }
        int max_score = INT_MIN;

        for (int h = 0; h != hsp_list->hspcnt; ++h) {
            int hsp_score = hsp_list->hsp_array[h]->score;
            log(jenv, jthis, LOG_DEBUG, "      HSP #%d hsp_score=%d (0x%x)", h, hsp_score,
                hsp_score);
            if (max_score < hsp_score)
                max_score = hsp_score;
        }
        max_scores.push_back(max_score);
        score_set.insert(max_score);

        log(jenv, jthis, LOG_DEBUG, "  have %lu max_scores", max_scores.size());
        log(jenv, jthis, LOG_DEBUG, "  have %lu in score_set", score_set.size());
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
    num_tuples = hsp_lists.size();
    if ((int)score_set.size() > topn) {
        int top = topn;
        for (auto sit = score_set.rbegin(); sit != score_set.rend(); ++sit) {
            --top;
            if (!top) {
                min_score = *sit;
                break;
            }
        }

        log(jenv, jthis, LOG_DEBUG, "  min_score is %d", min_score);

        for (size_t i = 0; i != max_scores.size(); ++i)
            if (max_scores[i] >= min_score)
                ++num_tuples;

        log(jenv, jthis, LOG_DEBUG, "  num_tuples is %lu", num_tuples);
    }

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
    for (size_t i = 0; i != hsp_lists.size(); ++i) {
        if (max_scores[i] >= min_score) {
            /*
               apply filtering
            if max_scores [ i ] >= min-score
                declare hsp_list := hsp_lists [ i ];

                // we now have enough information to have the JVM allocate
                // our tuple. It is proper to include the oid in this tuple,
                // because it is supposed to have an unique constraint on it
                // as a set key, and will be useful for logging. But
            otherwise,
                // our Java code won't need it.

                // "**" if we are using an integer packing format for the
            HSPs,  the work would need to be done up front so we know the
            */
            const BlastHSPList* hsp_list = hsp_lists[i];
            size_t blob_size = hsp_list->hspcnt * sizeof(ncbi::blast::SFlatHSP);

            ncbi::blast::SFlatHSP* hspblob = (ncbi::blast::SFlatHSP*)malloc(blob_size);
            if (!hspblob)
                throw std::runtime_error("Couldn't allocate hspblob");

            try {
                /*
                  declare tuple := JVM . allocateTuple ( job, hsp_list . oid,
                  max_scores [ i ], hsp_list . num_hsps * HSP_STRUCT_SIZE
                  )

                  // the JVM will have allocated memory for us in the form of
                  // a byte[] used as a blob for the { hsps }
                  */
                log(jenv, jthis, LOG_DEBUG, "  blob #%d size is %lu", i, blob_size);
                jbyteArray tuple = jenv->NewByteArray(blob_size);
                if (!tuple)
                    throw std::runtime_error("Couldn't create ByteArray");

                /*
                  declare hsp_blob: a sequence of HSP tuples, each with
                  size
                  HSP_STRUCT_SIZE
                  hsp_blob := tuple . hsp_blob

                  // we now record the HSP data within the blob
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
                int oid = hsp_list->oid;
                for (int h = 0; h != hsp_list->hspcnt; ++h) {
                    // FIX - don't need/want oid in the wire protocol
                    // Passed to traceback as argument, returned?
                    hspblob[idx].oid = oid;
                    hspblob[idx].score = hsp_list->hsp_array[h]->score;
                    hspblob[idx].query_start = hsp_list->hsp_array[h]->query.offset;
                    hspblob[idx].query_end = hsp_list->hsp_array[h]->query.end;
                    hspblob[idx].query_frame = hsp_list->hsp_array[h]->query.frame;
                    hspblob[idx].query_gapped_start = hsp_list->hsp_array[h]->query.gapped_start;
                    hspblob[idx].subject_start = hsp_list->hsp_array[h]->subject.offset;
                    hspblob[idx].subject_end = hsp_list->hsp_array[h]->subject.end;
                    hspblob[idx].subject_frame = hsp_list->hsp_array[h]->subject.frame;
                    hspblob[idx].subject_gapped_start
                        = hsp_list->hsp_array[h]->subject.gapped_start;

                    ++idx;
                }

                // Copy hspblob into Java tuple
                jenv->SetByteArrayRegion(tuple, 0, blob_size, (const jbyte*)hspblob);
                /* done with this tuple
                   tuples [ j ] := tuple
                   j := j + 1
                */

                // Create a new HSP_LIST
                jobject hspl_obj
                    = jenv->NewObject(hsplclass, hspl_ctor_id, oid, max_scores[i], tuple);
                if (!hspl_obj)
                    throw std::runtime_error("Couldn't make new HSP_LIST");

                jenv->SetObjectArrayElement(retarray, i, hspl_obj);
            }
            catch (...) {
                free(hspblob);
                throw;
            }

            free(hspblob);
        }
    }
    // Return tuples to Java object
    // done
    return retarray;

    /*
    The pseudo code above has the properties of only allocating memory in the
    JVM, and only when necessary, and only in the amounts necessary. It also
    has the property of creating the correct units for reduce.

    The same procedure would be applied for the results of traceback, except
    of course that you would use bottom-N evalues instead of top-N scores. It
    almost begs for abstracting the "max-scores" array and "score-set" so that
    they do the right thing.
    */
}

static void whack_hsp_lists(std::vector<BlastHSPList*>& hsp_lists)
{
    size_t i, count = hsp_lists.size();
    for (i = 0; i < count; ++i)
        Blast_HSPListFree(hsp_lists[i]);
}

static jobjectArray prelim_search(JNIEnv* jenv, jobject jthis, const char* jquery,
                                  const char* jdb_spec, const char* jprogram, const char* jparams,
                                  jint topn)
{
    if (jenv->EnsureLocalCapacity(64))
        throw std::runtime_error("Can't ensure local capacity");

    log(jenv, jthis, LOG_DEBUG,
        "Blast prelim_search called with\n"
        "  query   : %s\n"
        "  db_spec : %s\n"
        "  program : %s\n"
        "  topn    : %d\n",
        jquery, jdb_spec, jprogram, topn);
    //      "  params  : %s"

    ncbi::blast::TBlastHSPStream* hsp_stream = ncbi::blast::PrelimSearch(
        std::string(jquery), std::string(jdb_spec), std::string(jprogram));

    log(jenv, jthis, LOG_INFO, "Blast prelim_search returned");

    if (!hsp_stream) {
        log(jenv, jthis, LOG_ERROR, "NULL hsp_stream");
        throw std::runtime_error("prelim_search - NULL hsp_stream");
    }

    std::vector<BlastHSPList*> hsp_lists;

    try {
        log(jenv, jthis, LOG_DEBUG, "Begin BlastHSPStreamRead loop");
        while (1) {
            BlastHSPList* hsp_list = 0;
            int status = BlastHSPStreamRead(hsp_stream->GetPointer(),
                                            &hsp_list);  // FIX, use operator ->?

            if (status == kBlastHSPStream_Error) {
                log(jenv, jthis, LOG_ERROR, "kBlastHSPStream_Error");
                throw std::runtime_error("prelim_search - Exception from BlastHSPStreamRead");
            }

            if (status != kBlastHSPStream_Success || !hsp_list)
                break;

            hsp_lists.push_back(hsp_list);
            log(jenv, jthis, LOG_DEBUG, "  loop");
        }

        log(jenv, jthis, LOG_DEBUG, "  loop complete");
    }
    catch (...) {
        whack_hsp_lists(hsp_lists);
        log(jenv, jthis, LOG_ERROR, "exception in loop");
        //        BlastHSPStreamFree(hsp_stream);
        throw;
    }
    jobjectArray ret = iterate_HSPs(jenv, jthis, hsp_lists, topn);

    // FIX: Smart pointer eliminates these?
    whack_hsp_lists(hsp_lists);
    //   BlastHSPStreamFree(hsp_stream); // FIX?

    return ret;
}

/*
 * Class:     gov_nih_nlm_ncbi_blastjni_BLAST_LIB
 * Method:    prelim_search
 * Signature:
 * (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILgov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST;)Z
 */
JNIEXPORT jobjectArray JNICALL Java_gov_nih_nlm_ncbi_blastjni_BLAST_1LIB_prelim_1search(
    JNIEnv* jenv, jobject jthis, jstring jquery, jstring jdb_spec, jstring jprogram,
    jstring jparams, jint topn)
{
    uint32_t xtype = xc_no_err;

    const char* query = jenv->GetStringUTFChars(jquery, 0);
    const char* db_spec = jenv->GetStringUTFChars(jdb_spec, 0);
    const char* program = jenv->GetStringUTFChars(jprogram, 0);
    const char* params = jenv->GetStringUTFChars(jparams, 0);

    log(jenv, jthis, LOG_INFO, "C++ jni_prelim_1search called with");
    log(jenv, jthis, LOG_INFO, "  query   : %s", query);
    log(jenv, jthis, LOG_INFO, "  db_spec : %s", db_spec);
    log(jenv, jthis, LOG_INFO, "  program : %s", program);
    log(jenv, jthis, LOG_INFO, "  params  : %s", params);
    log(jenv, jthis, LOG_INFO, "  topn    : %d", topn);
    jobjectArray ret = NULL;
    try {
        ret = prelim_search(jenv, jthis, query, db_spec, program, params, topn);
    }
    catch (std::exception& x) {
        jni_throw(jenv, xtype = xc_java_exception, "%s", x.what());
    }
    // FIX - https://ncbiconfluence.ncbi.nlm.nih.gov/pages/viewpage.action?spaceKey=BLASTGCP&title=Errors+reported+by+BLAST
    /*
    catch (ncbi::blast::CInputException& x) {
        jni_throw(jenv, xtype = xc_blast_exception, "%s", x.GetMsg().data());
    }
    catch (CException& x) {
        jni_throw(jenv, xtype = xc_blast_exception, "%s", x.GetMsg().data());
    }
    */
    catch (...) {
        jni_throw(jenv, xtype = xc_java_runtime_exception, "%s - unknown exception", __func__);
    }

    log(jenv, jthis, LOG_INFO, "C++ prelim_search done");
    jenv->ReleaseStringUTFChars(jquery, query);
    jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
    jenv->ReleaseStringUTFChars(jprogram, program);
    jenv->ReleaseStringUTFChars(jparams, params);
    return ret;
}

/* Traceback Functions */
static jobjectArray traceback(JNIEnv* jenv, jobject jthis, const char* jquery, const char* jdb_spec,
                              const char* jprogram, jobjectArray hspl_obj)
{
    log(jenv, jthis, LOG_INFO, "Blast traceback called with");
    log(jenv, jthis, LOG_INFO, "  query    : %s", jquery);
    log(jenv, jthis, LOG_INFO, "  db_spec  : %s", jdb_spec);
    log(jenv, jthis, LOG_INFO, "  program  : %s", jprogram);

    jsize hspl_sz = jenv->GetArrayLength(hspl_obj);
    log(jenv, jthis, LOG_INFO, "  hsp_lists: %d", hspl_sz);

    jclass hsplclass = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST");
    if (!hsplclass)
        throw std::runtime_error("Can't get hspl class");

    jfieldID hspl_blob_fid = jenv->GetFieldID(hsplclass, "hsp_blob", "[B");
    if (!hspl_blob_fid)
        throw std::runtime_error("Can't get hsp_blob fieldID");

    jint oid = -1;
    std::vector<ncbi::blast::SFlatHSP> flat_hsp_list;
    // Iterate through HSP_LIST[]
    for (int h = 0; h != hspl_sz; ++h) {
        jobject hspl = jenv->GetObjectArrayElement(hspl_obj, h);
        if (!hspl)
            throw std::runtime_error("Couldn't get array element");

        jobject blobobj = jenv->GetObjectField(hspl, hspl_blob_fid);
        if (!blobobj)
            throw std::runtime_error("Couldn't get blob array");
        jbyteArray blobarr = (jbyteArray)blobobj;

        size_t blob_size = jenv->GetArrayLength(blobarr);

        jbyte* be = jenv->GetByteArrayElements(blobarr, NULL);
        if (be == NULL)
            throw std::runtime_error("Couldn't get bytearray");
        ncbi::blast::SFlatHSP* flathsps = (ncbi::blast::SFlatHSP*)be;

        size_t elements = blob_size / sizeof(ncbi::blast::SFlatHSP);
        log(jenv, jthis, LOG_DEBUG, "  blob will have %lu elements: %lu bytes", elements,
            blob_size);

        for (size_t i = 0; i != elements; ++i) {
            ncbi::blast::SFlatHSP flathsp = flathsps[i];
            flat_hsp_list.push_back(flathsp);
        }
        jenv->ReleaseByteArrayElements(blobarr, be, JNI_ABORT);  // Didn't update the array
    }

    ncbi::blast::TIntermediateAlignments alignments;
    int result = ncbi::blast::TracebackSearch(std::string(jquery), std::string(jdb_spec),
                                              std::string(jprogram), flat_hsp_list, alignments);
    size_t num_alignments = alignments.size();
    log(jenv, jthis, LOG_INFO, "Blast traceback returned status=%d. Got %d alignments", result,
        num_alignments);

    // Get class for TB_LIST
    jclass tbcls = jenv->FindClass("gov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST");
    if (!tbcls)
        throw std::runtime_error("Can't get tb class");

    jmethodID tb_ctor_id = jenv->GetMethodID(tbcls, "<init>", "(ID[B)V");
    if (!tb_ctor_id)
        throw std::runtime_error("Can't find tb ctor method");

    jobjectArray retarray = jenv->NewObjectArray(num_alignments, tbcls, NULL);
    for (size_t i = 0; i != alignments.size(); ++i) {
        jdouble evalue = alignments[i].first;
        std::string asn = alignments[i].second;
        oid = flat_hsp_list[i].oid;

        log(jenv, jthis, LOG_DEBUG, "  evalue=%f, oid=%d, ASN is %lu bytes", evalue, oid,
            asn.size());

        jbyteArray asn_blob = jenv->NewByteArray(asn.size());
        if (!asn_blob)
            throw std::runtime_error("Can't make bytearray");
        jenv->SetByteArrayRegion(asn_blob, 0, asn.size(), (const jbyte*)asn.data());
        jobject tb_obj = jenv->NewObject(tbcls, tb_ctor_id, oid, evalue, asn_blob);
        if (!tb_obj)
            throw std::runtime_error("Couldn't make new TB_LIST");

        jenv->SetObjectArrayElement(retarray, i, tb_obj);
    }

    return retarray;
}

/*
 * Class:     gov_nih_nlm_ncbi_blastjni_BLAST_LIB
 * Method:    traceback
 * Signature:
 * (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Lgov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST;Lgov/nih/nlm/ncbi/blastjni/BLAST_JOB;)[Lgov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST;
 */
JNIEXPORT jobjectArray JNICALL Java_gov_nih_nlm_ncbi_blastjni_BLAST_1LIB_traceback(
    JNIEnv* jenv, jobject jthis, jstring jquery, jstring jdb_spec, jstring jprogram,
    jobjectArray hspl)
{
    uint32_t xtype = xc_no_err;

    const char* query = jenv->GetStringUTFChars(jquery, 0);
    const char* db_spec = jenv->GetStringUTFChars(jdb_spec, 0);
    const char* program = jenv->GetStringUTFChars(jprogram, 0);

    log(jenv, jthis, LOG_DEBUG, "C++ jni_traceback called with");
    log(jenv, jthis, LOG_DEBUG, "  query   : %s", query);
    log(jenv, jthis, LOG_DEBUG, "  db_spec : %s", db_spec);
    log(jenv, jthis, LOG_DEBUG, "  program : %s", program);
    //    log(jenv, jthis, jlog_method, "  blob    : %lu bytes", blob_size);
    jobjectArray ret = NULL;
    try {
        ret = traceback(jenv, jthis, query, db_spec, program, hspl);
    }
    catch (std::exception& x) {
        jni_throw(jenv, xtype = xc_java_exception, "%s", x.what());
    }
    catch (...) {
        jni_throw(jenv, xtype = xc_java_runtime_exception, "%s - unknown exception", __func__);
    }

    log(jenv, jthis, LOG_INFO, "C++ traceback done");
    jenv->ReleaseStringUTFChars(jquery, query);
    jenv->ReleaseStringUTFChars(jdb_spec, db_spec);
    jenv->ReleaseStringUTFChars(jprogram, program);
    return ret;
}
