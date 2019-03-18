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


#include <objects/seqalign/Seq_align.hpp>
#include <serial/serial.hpp>
#include <serial/serialbase.hpp>
#include <serial/objistr.hpp>
#include <serial/objistrasnb.hpp>
#include <blastjni.hpp>

#include <iostream>
#include <string>

#define BOOST_TEST_MODULE blastjni
#include <boost/test/included/unit_test.hpp>


using namespace ncbi;
using namespace ncbi::blast;
using namespace objects;
using namespace std;

class CBlast4SparkFixture
{
public:

    string m_ProtQuery;
    string m_NuclQuery;
    string m_ProtDb;
    string m_NuclDb;
    int m_TopNPrelim;
    int m_TopNTraceback;

    CBlast4SparkFixture() : m_ProtQuery(ReadFile("data/prot_query.asn")),
                            m_NuclQuery(ReadFile("data/nucl_query.asn")),
                            m_ProtDb("data/prot_w_aux"),
                            m_NuclDb("data/nucl_w_aux"),
                            m_TopNPrelim(100),
                            m_TopNTraceback(100)
    {}

    // Read a whole text file into a string
    string ReadFile(const string& filename)
    {
        std::ifstream istr(filename);
        std::stringstream ss;
        ss << istr.rdbuf();
        return ss.str();
    }
};



BOOST_FIXTURE_TEST_SUITE(blastjni, CBlast4SparkFixture)

// Test a simple protein search with default parameters
BOOST_AUTO_TEST_CASE(ProteinSearch)
{
    string program = "blastp";
    string params = "";

    // expected alignment scores
    vector<int> expected_scores = {908, 285, 275};

    // expected alignment E-values
    vector<double> expected_evalues = {8.72170e-119, 1.81726e-26, 4.66216e-25};

    // expected number of alignments for each subject
    vector<int> expected_num_alignments = {1, 1, 1};

    // expected E-values encoded as integers
    vector<int> expected_int4evalues = {2718418, 592698, 560251};

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_NO_THROW(
       results = searchandtb(m_ProtQuery, m_ProtDb, program, params,
                             m_TopNPrelim, m_TopNTraceback));

    // test number of results
    BOOST_REQUIRE_EQUAL(3u, results.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_num_alignments.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_int4evalues.size());

    int idx = 0;
    // for each search result test score and E-value
    for (size_t i=0;i < results.size();i++) {

        CObjectIStreamAsnBinary in(results[i].second.c_str(),
                                   results[i].second.size());
        int num_alignments = 0;
        while (in.HaveMoreData()) {
            CRef<CSeq_align> sa(new CSeq_align);
            in >> *sa;
            double evalue;
            int score;
            BOOST_REQUIRE(sa->GetNamedScore(CSeq_align::eScore_EValue, evalue));
            BOOST_REQUIRE(sa->GetNamedScore(CSeq_align::eScore_Score, score));
            BOOST_REQUIRE_EQUAL(score, expected_scores[idx]);


            // Database size for SPARK is currently hard coded to 54105829280
            // in blast4spark.cpp. This is to represent full NR size. This test
            // may fail when this changes.
            BOOST_REQUIRE_CLOSE(evalue, expected_evalues[idx], 0.01);

            num_alignments++;
            idx++;
        }
        // test number of alignments for the same subject
        BOOST_REQUIRE_EQUAL(num_alignments, expected_num_alignments[i]);

        // test int4 E-value
        BOOST_REQUIRE_EQUAL(results[i].first[0], expected_int4evalues[i]);
    }
}


// Test a simple nucleotide search with default parameters
BOOST_AUTO_TEST_CASE(NucleotideSearch)
{
    string program = "blastn";
    string params = "";

    // expected alignment scores
    vector<int> expected_scores = {2630, 2038, 170, 113, 1341, 278,
                                   186, 139, 1186, 494, 242, 600};

    // expected alignment E-values
    vector<double> expected_evalues = {0.0, 0.0, 6.46661e-81, 3.13908e-49, 0.0,
                                       5.94029e-141, 8.24757e-90, 1.10530e-63,
                                       0.0, 0.0, 6.11079e-121, 0.0};

    // expected number of alignments for each subject
    vector<int> expected_num_alignments = {1, 3, 4, 3, 1};

    // expected E-values encoded as integers
    vector<int> expected_int4evalues = {4200000, 4200000, 4200000, 4200000,
                                        4200000};

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_NO_THROW(
       results = searchandtb(m_NuclQuery, m_NuclDb, program, params,
                             m_TopNPrelim, m_TopNTraceback));

    // test number of search results
    BOOST_REQUIRE_EQUAL(5u, results.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_num_alignments.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_int4evalues.size());

    int idx = 0;
    // for each result test score and E-value
    for (size_t i=0;i < results.size();i++) {

        CObjectIStreamAsnBinary in(results[i].second.c_str(),
                                   results[i].second.size());
        int num_alignments = 0;
        while (in.HaveMoreData()) {
            CRef<CSeq_align> sa(new CSeq_align);
            in >> *sa;
            double evalue;
            int score;
            BOOST_REQUIRE(sa->GetNamedScore(CSeq_align::eScore_EValue, evalue));
            BOOST_REQUIRE(sa->GetNamedScore(CSeq_align::eScore_Score, score));
            BOOST_REQUIRE_EQUAL(score, expected_scores[idx]);

            // Database size for SPARK is currently hard coded to 54105829280
            // in blast4spark.cpp. This is to represent full NR size. This test
            // may fail when this changes.
            BOOST_REQUIRE_CLOSE(evalue, expected_evalues[idx], 0.01);

            num_alignments++;
            idx++;
        }
        // test number of alignments for the same subject
        BOOST_REQUIRE_EQUAL(num_alignments, expected_num_alignments[i]);

        // test int4 E-value
        BOOST_REQUIRE_EQUAL(results[i].first[0], expected_int4evalues[i]);
    }
}


BOOST_AUTO_TEST_CASE(BadQuery)
{
    string program = "blastp";
    string params = "";

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_THROW(
       results = searchandtb("aaa", m_ProtDb, program, params,
                             m_TopNPrelim, m_TopNTraceback),
                             CException);
}


BOOST_AUTO_TEST_CASE(BadDatabase)
{
    string program = "blastp";
    string params = "";

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_THROW(
       results = searchandtb(m_ProtQuery, "somedb", program, params,
                             m_TopNPrelim, m_TopNTraceback),
                             CException);
}


BOOST_AUTO_TEST_CASE(BadProgram)
{
    string program = "someprogram";
    string params = "";

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_THROW(
       results = searchandtb(m_ProtQuery, m_ProtDb, program, params,
                             m_TopNPrelim, m_TopNTraceback),
                             CException);
}


BOOST_AUTO_TEST_CASE(MismatchedProgram)
{
    string program = "blastn";
    string params = "";

    ncbi::blast::TIntermediateAlignmentsTie results;
    BOOST_REQUIRE_THROW(
       results = searchandtb(m_ProtQuery, m_ProtDb, program, params,
                             m_TopNPrelim, m_TopNTraceback),
                             CException);
}



BOOST_AUTO_TEST_SUITE_END()
