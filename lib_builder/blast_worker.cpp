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
#include "json.hpp"
#include <ctype.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <streambuf>
#include <string>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

using json = nlohmann::json;

std::mutex                QUERIES_MUTEX;
std::queue< std::string > QUERIES;
int                       PARTITION_NUM;

void start_thread(int threadnum);

void start_thread(int threadnum)
{
    struct timeval tv_cur;

    fprintf(stderr, "pid %d Starting thread # %d\n", getpid(), threadnum);

    while (true)
    {
        std::string jsonfname;
        {
            std::lock_guard< std::mutex > guard(QUERIES_MUTEX);
            if (QUERIES.empty())
            {
                fprintf(stderr, "Thread %d, QUERIES queue empty\n", threadnum);
                return;
            }
            jsonfname = QUERIES.front();
            QUERIES.pop();
            fprintf(stderr, "Thread %d, queue has %zu entries\n", threadnum,
                    QUERIES.size());
        }

        fprintf(stderr, "pid %d Thread %d, loading %s\n", getpid(), threadnum,
                jsonfname.data());
        std::ifstream     jsonfile(jsonfname);
        std::stringstream buffer;
        buffer << jsonfile.rdbuf();
        std::string jsontext = buffer.str();

        json j;
        try
        {
            j = json::parse(jsontext);
        }
        catch (json::parse_error & e)
        {
            fprintf(stderr, "JSON parse error: %s", e.what());
            return;
        }

        std::cout << j.dump(4) << std::endl;

        std::string RID             = j["RID"];
        std::string query           = j["query_seq"];
        std::string db_spec         = j["db_tag"];
        std::string program         = j["program"];
        std::string params          = j["blast_params"];
        int         top_n_prelim    = j["top_N_prelim"];
        int         top_n_traceback = j["top_N_traceback"];

        std::string db = std::string(db_spec, 0, 2);
        std::string location
            = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/"
              "50M/";

        // for (int i = 0; i != PARTITION_NUM; ++i)
        int i = PARTITION_NUM;
        {
            gettimeofday(&tv_cur, NULL);
            unsigned long starttime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;

            char dbloc[1024];

            if (db == "nr")
            {
                sprintf(dbloc, "%snr_50M.%02d", location.data(), i);
            }
            else if (db == "nt")
            {
                sprintf(dbloc, "%snt_50M.%02d", location.data(), i);
            }
            else
            {
                std::cerr << "Unknown db:" << db << "\n";
                return;
            }
            fprintf(stderr, "pid %d Thread %d, RID %s, dbloc is %s\n", getpid(),
                    threadnum, RID.data(), dbloc);
            auto alignments
                = searchandtb(query, std::string(dbloc), program, params,
                              top_n_prelim, top_n_traceback);
            fprintf(stderr, "Thread %d, RID %s, got back %zu for %s\n",
                    threadnum, RID.data(), alignments.size(), dbloc);

            gettimeofday(&tv_cur, NULL);
            unsigned long finishtime = tv_cur.tv_sec * 1000000 + tv_cur.tv_usec;
            fprintf(
                stderr,
                "pid %d Thread %d, RID %s, blast_worker called  PrelimSearch, "
                "took %lu ms\n",
                getpid(), threadnum, RID.data(),
                (finishtime - starttime) / 1000);
        }
    }
}

int main(int argc, char * argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0]
                  << " #threads partition# query.json ...\n";
        return 1;
    }

    int num_threads = atoi(argv[1]);
    PARTITION_NUM   = atoi(argv[2]);

    {
        std::lock_guard< std::mutex > guard(QUERIES_MUTEX);
        for (int arg = 3; arg != argc; ++arg)
        {
            const char * jsonfname = argv[arg];
            QUERIES.push(std::string(argv[arg]));
        }
    }
    fprintf(stderr, "QUERIES has %zu\n", QUERIES.size());

    std::vector< std::thread * > threads;
    for (int i = 0; i != num_threads; ++i)
    {
        std::thread * thrd = new std::thread(start_thread, i);
        threads.push_back(thrd);
    }

    for (auto thrd : threads)
    {
        thrd->join();
    }

    fprintf(stderr, "Process finished\n");

    return 0;
}
