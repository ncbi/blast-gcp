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

#include "blast4spark.hpp"
#include <algo/blast/api/blast_advprot_options.hpp>
#include <algo/blast/api/blast_exception.hpp>
#include <algo/blast/api/blast_nucl_options.hpp>
#include <algo/blast/api/blast_results.hpp>
#include <algo/blast/api/local_blast.hpp>
#include <algo/blast/api/objmgrfree_query_data.hpp>
#include <algo/blast/api/prelim_stage.hpp>
#include <algo/blast/core/blast_hspstream.h>
#include <ncbi_pch.hpp>
#include <objects/seq/Bioseq.hpp>
#include <objects/seq/Seq_data.hpp>
#include <objects/seqalign/Seq_align.hpp>
#include <objects/seqalign/Seq_align_set.hpp>
#include <objects/seqloc/Seq_id.hpp>
#include <objects/seqset/Bioseq_set.hpp>
#include <objects/seqset/Seq_entry.hpp>
#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>

/** @addtogroup AlgoBlast
 *
 * @{
 */
#if 0
BEGIN_NCBI_SCOPE
USING_SCOPE(objects);
BEGIN_SCOPE(blast)

BlastHSPStream*
PrelimSearch(const std::string& single_query, 
             const std::string& database_name,
             const std::string& program_name)
{
    bool is_query_protein = false;
    bool is_db_protein = false;

    if (program_name == "blastp") {
        is_query_protein = true;
        is_db_protein = true;
    }
    else if (program_name == "blastn") {
        is_query_protein = false;
        is_db_protein = false;
    }
    else {
        // only blastp and blastn at this point
        return NULL;
    }

    CRef<CBioseq_set> bioseq_set(new CBioseq_set);
    CRef<CSeq_entry> seq_entry(new CSeq_entry);
    CBioseq& bioseq = seq_entry->SetSeq();
    bioseq.SetId().clear();
    CRef<CSeq_id> seqid(new CSeq_id("lcl|query"));
    bioseq.SetId().push_back(seqid);
    bioseq.SetDescr();
    bioseq.SetInst().SetRepr(CSeq_inst::eRepr_raw);
    bioseq.SetInst().SetLength(single_query.length());


    if (is_query_protein) {

        bioseq.SetInst().SetMol(CSeq_inst::eMol_aa);
        vector<char>& query_seq = bioseq.SetInst().SetSeq_data().SetNcbistdaa().Set();
        query_seq.resize(single_query.length());


        for (size_t i=0;i < single_query.length();i++) {
            query_seq[i] = AMINOACID_TO_NCBISTDAA[(int)single_query[i]];
        }
    }
    else {
        bioseq.SetInst().SetMol(CSeq_inst::eMol_na);
        string& query_seq = bioseq.SetInst().SetSeq_data().SetIupacna().Set();
        query_seq.resize(single_query.length());
        memcpy(&query_seq[0], &single_query[0],
               single_query.length() * sizeof(char));
    }

    bioseq_set->SetSeq_set().push_back(seq_entry);
    CRef<IQueryFactory> query_batch(new CObjMgrFree_QueryFactory(bioseq_set));

    CRef<CBlastOptionsHandle> opts_hndl;
    CRef<CBlastOptions> opts;
    auto_ptr<CSearchDatabase> db;

    BlastHSPStream* hsp_stream = NULL;

    try {
        if (is_db_protein) {
            opts_hndl.Reset(new CBlastAdvancedProteinOptionsHandle);

            // these set database size for NR for proper statistics and score
            // cutoffs
            opts_hndl->SetOptions().SetDbLength(54105829280L);
            opts_hndl->SetOptions().SetDbSeqNum(147618291);

            db.reset(new CSearchDatabase(database_name,
                                         CSearchDatabase::eBlastDbIsProtein));
        }
        else {
            opts_hndl.Reset(new CBlastNucleotideOptionsHandle);

            // these set database size for NT for proper statistics and score
            // cutoffs
            opts_hndl->SetOptions().SetDbLength(174044644244L);
            opts_hndl->SetOptions().SetDbSeqNum(46882714);

            db.reset(new CSearchDatabase(database_name,
                                         CSearchDatabase::eBlastDbIsNucleotide));
        }

        opts.Reset(&opts_hndl->SetOptions());

        CBlastPrelimSearch prelim(query_batch, opts, *db);
        CRef<SInternalData> data = prelim.Run();

        hsp_stream = data->m_HspStream.Release()->GetPointer();
        BlastHSPStreamClose(hsp_stream);
    }
    catch (CException& e) {

        // TODO: add error handling
        printf ( "caught exception: '%s'\n", e.what() );
        return NULL;
    }

    return hsp_stream;
}


END_SCOPE(blast)
END_NCBI_SCOPE
#endif

static void iterate_HSPs(BlastHSPList* hsp_list)
{
    for (int i = 0; i < hsp_list->hspcnt; ++i) {
        const BlastHSP* hsp = hsp_list->hsp_array[i];
        // printf("%s - have HSP [ %d ] at %p:\n", __func__, i, hsp);
        printf("%s - have HSP [ %d ] :\n", __func__, i);
        printf(
            "  oid = %d\n"
            "  score = %d\n"
            "  query\n  {\n"
            "    frame = %d\n"
            "    offset = %d\n"
            "    end = %d\n"
            "  }\n  subject\n  {\n"
            "    frame = %d\n"
            "    offset = %d\n"
            "    end = %d\n"
            "  }\n\n",
            hsp_list->oid, hsp->score, hsp->query.frame, hsp->query.offset,
            hsp->query.end, hsp->subject.frame, hsp->subject.offset,
            hsp->subject.end);
    }
}

static int run(int argc, char* argv[])
{
    std::string jobid(argv[1]);
    std::string query(argv[2]);
    std::string db(argv[3]);
    std::string params(argv[4]);

    BlastHSPStream* hsp_stream = ncbi::blast::PrelimSearch(query, db, params);
    // printf("Returned %p\n", hsp_stream);

    if (hsp_stream != NULL) {
        BlastHSPList* hsp_list = NULL;
        int status = BlastHSPStreamRead(hsp_stream, &hsp_list);
        //        printf("BlastHSPStreamRead returned status = %d ( %p )\n",
        //        status, hsp_stream);
        printf("BlastHSPStreamRead returned status = %d\n", status);
        while (status == kBlastHSPStream_Success && hsp_list != NULL) {
            //            printf("%s - have hsp_list at %p\n", __func__,
            //            hsp_list);
            iterate_HSPs(hsp_list);
            status = BlastHSPStreamRead(hsp_stream, &hsp_list);
        }

        Blast_HSPListFree(hsp_list);
        BlastHSPStreamFree(hsp_stream);
    }

    return 0;
}

int main(int argc, char* argv[])
{
    if (argc != 5) {
        fprintf(stderr, "Usage: %s jobid query db params\n", argv[0]);
        return 1;
    }

    try {
        run(argc, argv);
    }
    catch (...) {
        return 1;
    }

    return 0;
}
