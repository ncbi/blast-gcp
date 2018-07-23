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

#include "json.hpp"
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
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fstream>
#include <iostream>
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
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <streambuf>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

static const char * LOGPATH = "/tmp/blast_server.log";
bool volatile TERM          = false;  // updated in signal handler
static void log(const char * loglevel, const char * fmt, ...);

using json = nlohmann::json;

struct blast_tb_list
{
    int                oid;
    std::vector< int > ties;
    std::vector< int > asn1_blob;
};

void sighandler(int signum);
void process(int fdsocket);

static void log(const char * loglevel, const char * fmt, ...)
{
    static FILE * fout = NULL;

    if (fout == NULL)
        fout = fopen(LOGPATH, "a");

    if (fout)
    {
        fprintf(fout, "%s (%d) : ", loglevel, getpid());

        va_list args;
        va_start(args, fmt);
        vfprintf(fout, fmt, args);
        va_end(args);

        fputc('\n', fout);
        fflush(fout);
    }
}

static void whack_hsp_lists(std::vector< BlastHSPList * > & hsp_lists)
{
    size_t i, count = hsp_lists.size();
    for (i = 0; i < count; ++i)
        Blast_HSPListFree(hsp_lists[i]);
}

static std::vector< ncbi::blast::SFlatHSP >
iterate_HSPs(std::vector< BlastHSPList * > & vBlastHSPList, int top_n)
{
    std::vector< int > max_scores;
    std::set< int >    score_set;
    size_t             num_tuples = 0;
    int                min_score  = INT_MIN;

    max_scores.reserve(500);

    for (const auto & pBlastHSPList : vBlastHSPList)
    {
        int max_score = INT_MIN;

        if (pBlastHSPList->hspcnt)
        {
            for (int h = 0; h != pBlastHSPList->hspcnt; ++h)
            {
                int hsp_score = pBlastHSPList->hsp_array[h]->score;
                if (max_score < hsp_score)
                    max_score = hsp_score;
            }
        }

        max_scores.push_back(max_score);
        score_set.insert(max_score);
    }

    log("INFO", "  score_set has %zu", score_set.size());

    if ((int)score_set.size() > top_n)
    {
        int top = top_n;
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
        num_tuples = vBlastHSPList.size();

    log("INFO", "  num_tuples is %zu, min_score is %d", num_tuples, min_score);

    std::vector< struct ncbi::blast::SFlatHSP > retarray;
    retarray.reserve(num_tuples * 2);

    for (size_t i = 0; i != vBlastHSPList.size(); ++i)
    {
        if (max_scores[i] >= min_score)
        {
            const BlastHSPList * pBlastHSPList = vBlastHSPList[i];

            for (int h = 0; h != pBlastHSPList->hspcnt; ++h)
            {
                struct ncbi::blast::SFlatHSP flathsp;
                flathsp.oid         = pBlastHSPList->oid;
                flathsp.score       = max_scores[i];
                flathsp.query_start = pBlastHSPList->hsp_array[h]->query.offset;
                flathsp.query_end   = pBlastHSPList->hsp_array[h]->query.end;
                flathsp.query_frame = pBlastHSPList->hsp_array[h]->query.frame;
                flathsp.query_gapped_start
                    = pBlastHSPList->hsp_array[h]->query.gapped_start;
                flathsp.subject_start
                    = pBlastHSPList->hsp_array[h]->subject.offset;
                flathsp.subject_end = pBlastHSPList->hsp_array[h]->subject.end;
                flathsp.subject_frame
                    = pBlastHSPList->hsp_array[h]->subject.frame;
                flathsp.subject_gapped_start
                    = pBlastHSPList->hsp_array[h]->subject.gapped_start;

                if (retarray.size() < 1000)
                    retarray.push_back(flathsp);
                else
                    log("WARN", "  Cutting off large vector %zu",
                        retarray.size());
            }
        }
    }

    return retarray;
}

void sighandler(int signum)
{
    log("ERROR", "Received signal %d", signum);
    TERM = true;
}

void process(int fdsocket)
{
    struct timeval    tv_cur;
    std::stringstream buffer;
    char              buf[4096];
    int               rc;

    sprintf(buf, "Welcome to blast_server\n");
    write(fdsocket, buf, strlen(buf));
    while ((rc = read(fdsocket, buf, sizeof(buf))) > 0)
    {
        log("INFO", " Read %zu bytes.", rc);
        buffer.write(buf, rc);
    }

    if (rc == -1)
    {
        log("ERROR", "Couldn't read from socket: %s", strerror(errno));
        return;
    }

    std::string jsontext = buffer.str();
    log("INFO", "Total read of %zu bytes", jsontext.size());
    log("INFO", "JSON read was '%s'", jsontext.data());

    json j;
    try
    {
        j = json::parse(jsontext);
    }
    catch (json::parse_error & e)
    {
        log("ERROR", "JSON Parse error: %s %d at byte %d", e.what(), e.id,
            e.byte);
        return;
    }

    int         top_n_prelim    = j["top_N_prelim"];
    int         top_n_traceback = j["top_N_traceback"];
    std::string query           = j["query_seq"];
    std::string db_location     = j["db_location"];
    std::string program         = j["program"];
    std::string params          = j["blast_params"];


    gettimeofday(&tv_cur, NULL);
    unsigned long starttime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;

    log("INFO", "blast_server calling PrelimSearch");

    ncbi::blast::TBlastHSPStream * hsp_stream
        = ncbi::blast::PrelimSearch(query, db_location, program, params);

    gettimeofday(&tv_cur, NULL);
    unsigned long finishtime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;
    log("INFO", "blast_server called  PrelimSearch, took %lu ms",
        (finishtime - starttime) / 1000);

    if (!hsp_stream)
    {
        log("INFO", "NULL hsp_stream");
    }

    std::vector< BlastHSPList * > vBlastHSPList;
    vBlastHSPList.reserve(500);

    try
    {
        while (1)
        {
            BlastHSPList * pBlastHSPList = NULL;
            int            status
                = BlastHSPStreamRead(hsp_stream->GetPointer(), &pBlastHSPList);

            if (status == kBlastHSPStream_Error)
            {
                log("ERROR", "kBlastHSPStream_Error");
                return;
            }

            if (status != kBlastHSPStream_Success || !pBlastHSPList)
            {
                break;
            }

            if (pBlastHSPList->oid != -1)
                vBlastHSPList.push_back(pBlastHSPList);
        }
    }
    catch (...)
    {
        log("ERROR", "exception in loop");
    }

    log("INFO", "  Have %zu vBlastHSPList", vBlastHSPList.size());

    std::vector< struct ncbi::blast::SFlatHSP > vSFlatHSP
        = iterate_HSPs(vBlastHSPList, top_n_prelim);

    whack_hsp_lists(vBlastHSPList);

    log("INFO", "Ignoring top_n_traceback=%d", top_n_traceback);


    ncbi::blast::TIntermediateAlignmentsTie alignments;
    gettimeofday(&tv_cur, NULL);
    starttime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;

    log("INFO", "blast_server calling TracebackSearch with %zu flat HSPs",

        vSFlatHSP.size());
    int result = ncbi::blast::TracebackSearch(query, db_location, program,
                                              params, vSFlatHSP, alignments);

    gettimeofday(&tv_cur, NULL);
    finishtime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;
    log("INFO",
        "blast_server called  TracebackSearch, took %lu ms returned %d, "
        "got %zu alignments",
        (finishtime - starttime) / 1000, result, alignments.size());

    std::vector< struct blast_tb_list > tbl;
    if (alignments.size() >= 1000)
        log("WARN", "  Cutting off large vector %zu", alignments.size());
    size_t cutoff = std::min(alignments.size(), (size_t)1000);
    tbl.reserve(cutoff);
    for (size_t i = 0; i != cutoff; ++i)
    {
        struct blast_tb_list btbl;
        btbl.oid  = vSFlatHSP[i].oid;
        btbl.ties = alignments[i].first;

        size_t blob_size = alignments[i].second.size();
        for (size_t b = 0; b != blob_size; ++b)
            btbl.asn1_blob.push_back(alignments[i].second[b]);

        tbl.push_back(btbl);
    }


    json jtbs;
    for (size_t i = 0; i != tbl.size(); ++i)
    {
        json jtbl;
        jtbl["oid"]       = tbl[i].oid;
        jtbl["ties"]      = tbl[i].ties;
        jtbl["asn1_blob"] = tbl[i].asn1_blob;
        jtbs.push_back(jtbl);
    }
    json jtblist;
    jtblist["protocol"]      = "traceback-results-1.0";
    jtblist["blast_tb_list"] = jtbs;
    std::string out          = jtblist.dump(2);
    ssize_t     ret          = write(fdsocket, out.data(), out.size());
    if (ret < (ssize_t)out.size())
        log("WARN", "Couldn't write everything");

    if (ret < 0)
        log("ERROR", "write returned %d: %s", errno, strerror(errno));
}

int main(int argc, char * argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " {TCP port}\n";
        return 1;
    }

    int tcp_port = strtol(argv[1], NULL, 10);
    if (tcp_port < 1024 || tcp_port > 65535)
    {
        std::cerr << "TCP port must be between 1024..65535\n";
        return 1;
    }

    int tcp_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_socket == -1)
    {
        perror("socket");
        return 2;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(tcp_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int enable = 1;
    if (setsockopt(tcp_socket, SOL_SOCKET, SO_REUSEADDR, &enable,
                   sizeof(enable))
        < 0)
    {
        perror("setsockopt SO_REUSEADDR");
        return 2;
    }

    int ret = bind(tcp_socket, (const struct sockaddr *)&addr, sizeof(addr));
    if (ret != 0)
    {
        if (errno == EADDRINUSE)
        {
            std::cerr << "Another server already running\n";
            return 0;
        }
        perror("bind");
        return 2;
    }

    if (listen(tcp_socket, 64) != 0)
    {
        if (errno == EADDRINUSE)
        {
            std::cerr << "Another server already running\n";
            return 0;
        }
        log("ERROR", "listen returned %d: %s", errno, strerror(errno));
        return 2;
    }

    // Fork daemon here
    struct sigaction new_action;
    new_action.sa_handler = sighandler;
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;
    sigaction(SIGABRT, &new_action, NULL);
    sigaction(SIGFPE, &new_action, NULL);
    sigaction(SIGBUS, &new_action, NULL);
    sigaction(SIGILL, &new_action, NULL);
    sigaction(SIGSEGV, &new_action, NULL);
    sigaction(SIGSYS, &new_action, NULL);
    sigaction(SIGINT, &new_action, NULL);
    sigaction(SIGTERM, &new_action, NULL);
    signal(SIGCHLD, SIG_IGN);

    pid_t pid = fork();
    if (pid < 0)
    {
        perror("Fork");
        return 2;
    }
    else if (pid > 0)  // Parent
    {
        fprintf(stderr,
                "blast_server daemon started (pid=%d)\n"
                "Listening on TCP port %d\n"
                "See %s for logs\n",
                pid, tcp_port, LOGPATH);
        return 0;
    }

    // Child
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    umask(0);
    setsid();

    // FIX: Terminate child if time exceeded? (alarm() trigger SIGALRM)

    while (!TERM)
    {
        log("INFO", "Parent daemon listening on TCP port %d", tcp_port);
        int fdsocket = accept(tcp_socket, NULL, NULL);
        if (fdsocket == -1)
        {
            log("ERROR", "accept returned %d: %s", errno, strerror(errno));
            continue;
        }

        pid = fork();
        if (pid < 0)
        {
            log("ERROR", "fork returned %d: %s", errno, strerror(errno));
            return 2;
        }
        else if (pid > 0)
        {
            log("INFO", "Forked child %d to handle request", pid);
            continue;
        }
        // We're the child
        log("INFO", "Child handling request");
        process(fdsocket);
        log("INFO", "Request handled, child exiting\n");
        shutdown(fdsocket, SHUT_RDWR);
        exit(0);
    }

    log("INFO", "Parent daemon terminating");

    return 0;
}
