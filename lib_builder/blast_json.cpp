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
#include <ncbi_pch.hpp>
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
#include <ncbi_pch.hpp>
#include <objects/seq/Bioseq.hpp>
#include <objects/seq/Seq_data.hpp>
#include <objects/seqalign/Seq_align.hpp>
#include <objects/seqalign/Seq_align_set.hpp>
#include <objects/seqloc/Seq_id.hpp>
#include <objects/seqset/Bioseq_set.hpp>
#include <objects/seqset/Seq_entry.hpp>
#include <ctype.h>
#include <fstream>
#include <pthread.h>
#include <set>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <streambuf>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

using json = nlohmann::json;

struct blast_hsp_list
{
    int oid;
    int max_score;
    char * hsp_blob;
    size_t blob_size;
};

struct blast_tb_list
{
    int oid;
    double evalue;
    std::vector<int> asn1_blob;
};

static std::vector<struct blast_hsp_list> iterate_HSPs( std::vector< BlastHSPList * > & hsp_lists, int topn )
{
    std::vector< int > max_scores;
    std::set< int >    score_set;
    size_t             num_tuples = 0;
    int                min_score  = INT_MIN;

    for (const auto & hsp_list : hsp_lists)
    {
        int max_score = INT_MIN;

        if ( hsp_list->hspcnt )
        {
            for ( int h = 0; h != hsp_list->hspcnt; ++h )
            {
                int hsp_score = hsp_list->hsp_array[h]->score;
                if ( max_score < hsp_score )
                    max_score = hsp_score;
            }
        }

        max_scores.push_back( max_score );
        score_set.insert( max_score );
    }

    fprintf(stderr,"score_set has %zu\n", score_set.size());

    if ( (int)score_set.size() > topn )
    {
        int top = topn;
        for ( auto sit = score_set.rbegin(); sit != score_set.rend(); ++sit )
        {
            --top;
            if ( !top )
            {
                min_score = *sit;
                break;
            }
        }

        for (const auto max_score : max_scores)
            if ( max_score >= min_score )
                ++num_tuples;
    }
    else
        num_tuples = hsp_lists.size();

    fprintf(stderr,"num_tuples is %zu, min_score is %d \n", num_tuples, min_score);

    std::vector<struct blast_hsp_list> retarray;

    for ( size_t i = 0; i != hsp_lists.size(); ++i )
    {
        if ( max_scores[i] >= min_score )
        {
            const BlastHSPList * hsp_list = hsp_lists[i];

            struct blast_hsp_list hspl;
            hspl.oid=hsp_list->oid;
            hspl.max_score=max_scores[i];

            size_t blob_size = hsp_list->hspcnt * sizeof( ncbi::blast::SFlatHSP );
            ncbi::blast::SFlatHSP * hsp_blob
                = (ncbi::blast::SFlatHSP *)malloc( blob_size ); // FIX: Free
            if ( !hsp_blob )
                throw std::runtime_error( "Couldn't allocate hsp_blob" );

            hspl.blob_size=blob_size;

            for ( int h = 0; h != hsp_list->hspcnt; ++h )
            {
                hsp_blob[h].oid   = hsp_list->oid;
                hsp_blob[h].score = hsp_list->hsp_array[h]->score;
                hsp_blob[h].query_start
                    = hsp_list->hsp_array[h]->query.offset;
                hsp_blob[h].query_end = hsp_list->hsp_array[h]->query.end;
                hsp_blob[h].query_frame
                    = hsp_list->hsp_array[h]->query.frame;
                hsp_blob[h].query_gapped_start
                    = hsp_list->hsp_array[h]->query.gapped_start;
                hsp_blob[h].subject_start
                    = hsp_list->hsp_array[h]->subject.offset;
                hsp_blob[h].subject_end
                    = hsp_list->hsp_array[h]->subject.end;
                hsp_blob[h].subject_frame
                    = hsp_list->hsp_array[h]->subject.frame;
                hsp_blob[h].subject_gapped_start
                    = hsp_list->hsp_array[h]->subject.gapped_start;
            }
            hspl.hsp_blob=(char *)hsp_blob;

            retarray.push_back(hspl);
        }
    }
    return retarray;
}


int main( int argc, char * argv[] )
{
    if ( argc != 3 )
    {
        std::cerr << "Usage: " << argv[0] << "{prelim_search|traceback} query.json\n";
        return 1;
    }

    const char * func = argv[1];
    bool prelim=false;
    bool traceback=false;
    if (!strcmp(func,"prelim_search"))
        prelim=true;
    else if (!strcmp(func,"traceback"))
        traceback=true;
    else
    {
        fprintf(stderr,"Unknown function: %s\n", func);
        return 1;
    }

    const char * jsonfname = argv[2];
    std::ifstream jsonfile( jsonfname );
    std::stringstream buffer;
    buffer << jsonfile.rdbuf();
    std::string jsontext = buffer.str();

    json j = json::parse( jsontext );

    //    std::cerr << j.dump(2) << std::endl;

    std::string query=j["query_seq"];
    std::string db_location=j["db_location"];
    std::string program=j["program"];
    std::string params=j["blast_params"];

    if (prelim)
    {
        int top_n_prelim=j["top_N_prelim"];
        fprintf(stderr,"Calling PrelimSearch\n");
        ncbi::blast::TBlastHSPStream * hsp_stream =
            ncbi::blast::PrelimSearch(query,
                                      db_location,
                                      program,
                                      params);
        fprintf(stderr,"Called  PrelimSearch\n");

        if ( !hsp_stream )
        {
            fprintf(stderr,"NULL hsp_stream" );
        }

        std::vector< BlastHSPList * > hsp_lists;

        try
        {
            while ( 1 )
            {
                BlastHSPList * hsp_list = NULL;
                int            status
                    = BlastHSPStreamRead( hsp_stream->GetPointer(), &hsp_list );

                if ( status == kBlastHSPStream_Error )
                {
                    fprintf(stderr, "ERROR kBlastHSPStream_Error" );
                    break;
                }

                if ( status != kBlastHSPStream_Success || !hsp_list )
                {
                    break;
                }

                if (hsp_list->oid != -1)
                    hsp_lists.push_back( hsp_list );
            }
        }
        catch ( ... )
        {
            fprintf(stderr, "ERROR exception in loop" );
        }

        fprintf(stderr,"Have %zu hsp_lists\n", hsp_lists.size());

        std::vector<struct blast_hsp_list> hspl= iterate_HSPs(hsp_lists, top_n_prelim);

        json hsps;
        for (size_t i=0; i!= hspl.size(); ++i)
        {
            json hsp;
            hsp["oid"]=hspl[i].oid;
            hsp["max_score"]=hspl[i].max_score;
            std::vector<int> blob;
            for (size_t b=0; b!=hspl[i].blob_size; ++b)
            {
                blob.push_back(hspl[i].hsp_blob[b]);
            }
            hsp["hsp_blob"]=blob;

            hsps.push_back(hsp);
        }
        json result;
        result["blast_hsp_list"]=hsps;
        std::cout << result.dump(2) << std::endl;
    } else if (traceback)
    {
        int top_n_traceback=j["top_N_traceback"];
        fprintf(stderr, "Ignoring top_n_traceback=%d\n", top_n_traceback);

        std::vector<ncbi::blast::SFlatHSP> flat_hsp_list;

        json jbhpl=j["blast_hsp_list"];
        for (size_t i=0; i!=jbhpl.size(); ++i)
        {
            json jhspl=jbhpl[i];

            struct blast_hsp_list bhspl;
            bhspl.oid=jhspl["oid"];
            bhspl.max_score=jhspl["max_score"];
            fprintf(stderr,"blob %zu oid=%d max_score=%d\n", i, bhspl.oid, bhspl.max_score);
            json jblob=jhspl["hsp_blob"];
            size_t blob_size=jblob.size();
            fprintf(stderr,"blob_size is %zu\n", blob_size);

            ncbi::blast::SFlatHSP * hsp_blob
                = (ncbi::blast::SFlatHSP *)malloc( blob_size ); // FIX: Free
            if ( !hsp_blob )
                throw std::runtime_error( "Couldn't allocate hsp_blob" );

            for (size_t b=0; b!=blob_size; ++b)
            {
                char * blob=(char *)hsp_blob;
                int v=jblob[b];
                blob[b]=(char)v;
            }

            size_t elements=blob_size / sizeof( ncbi::blast::SFlatHSP );
            for (size_t element=0; element!=elements; ++element)
            {
                flat_hsp_list.push_back(hsp_blob[element]);
            }
        }

        ncbi::blast::TIntermediateAlignments alignments;
        fprintf(stderr,"Calling TracebackSearch with %zu flat HSPs\n", flat_hsp_list.size());
        int  result = ncbi::blast::TracebackSearch(
                                                   query,
                                                   db_location,
                                                   program,
                                                   params,
                                                   flat_hsp_list,
                                                   alignments);
        fprintf(stderr,"Called  TracebackSearch, returned %d, got %zu alignments\n", result, alignments.size());

        std::vector<struct blast_tb_list> tbl;
        for (size_t i=0; i!=alignments.size(); ++i)
        {
            struct blast_tb_list btbl;
            btbl.oid=-1;
            btbl.evalue=alignments[i].first;
            size_t blob_size=alignments[i].second.size();
            for (size_t b=0; b!=blob_size; ++b)
                btbl.asn1_blob.push_back(alignments[i].second[b]);

            tbl.push_back(btbl);
        }


        json jtbs;
        for (size_t i=0; i!=tbl.size(); ++i)
        {
            json jtbl;
            jtbl["evalue"]=tbl[i].evalue;
            jtbl["asn1_blob"]=tbl[i].asn1_blob;
            jtbs.push_back(jtbl);
        }
        json jtblist;
        jtblist["blast_tb_list"]=jtbs;
        std::cout << jtblist.dump(2) << std::endl;
    }

    return 0;
}

