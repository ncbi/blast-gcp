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


    CBlast4SparkFixture() : m_ProtQuery(ReadFile("data/prot_query.asn")),
                            m_NuclQuery(ReadFile("data/nucl_query.asn")),
                            m_ProtDb("data/prot_w_aux"),
                            m_NuclDb("data/nucl_w_aux")

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
    int top_n_prelim = 100;
    int top_n_traceback = 100;

    // expected alignment scores
    vector<int> expected_scores = {908, 285, 275};

    // expected alignment E-values
    vector<double> expected_evalues = {8.72170e-119, 1.81726e-26, 4.66216e-25};

    ncbi::blast::TIntermediateAlignments results;
    BOOST_REQUIRE_NO_THROW(
       results = searchandtb(m_ProtQuery, m_ProtDb, program, params,
                             top_n_prelim, top_n_traceback));

    // test number of results
    BOOST_REQUIRE_EQUAL(3u, results.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_scores.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_evalues.size());

    // for each search result test score and E-value
    for (size_t i=0;i < results.size();i++) {
        CNcbiStrstream ss;
        ss << results[i].second;
        CSeq_align sa;
        ss >> MSerial_AsnBinary >> sa;
        double evalue;
        int score;
        BOOST_REQUIRE(sa.GetNamedScore(CSeq_align::eScore_EValue, evalue));
        BOOST_REQUIRE(sa.GetNamedScore(CSeq_align::eScore_Score, score));
        BOOST_CHECK_CLOSE( results[i].first, evalue, 0.0001 );
        BOOST_REQUIRE_EQUAL(score, expected_scores[i]);

        // Database size for SPARK is currently hard coded to 54105829280
        // in blast4spark.cpp. This is to represent full NR size. This test and
        // may fail when this changes.
        BOOST_REQUIRE_CLOSE(evalue, expected_evalues[i], 0.01);
    }
}


// Test a simple nucleotide search with default parameters
BOOST_AUTO_TEST_CASE(NucleotideSearch)
{
    string program = "blastn";
    string params = "";
    int top_n_prelim = 100;
    int top_n_traceback = 100;

    // expected alignment scores
    vector<int> expected_scores = {2630, 2038, 170, 113, 1341, 278,
                                   186, 139, 1186, 494, 242, 600};

    // expected alignment E-values
    vector<double> expected_evalues = {0.0, 0.0, 6.46661e-81, 3.13908e-49, 0.0,
                                       5.94029e-141, 8.24757e-90, 1.10530e-63,
                                       0.0, 0.0, 6.11079e-121, 0.0};

    ncbi::blast::TIntermediateAlignments results;
    BOOST_REQUIRE_NO_THROW(
       results = searchandtb(m_NuclQuery, m_NuclDb, program, params,
                             top_n_prelim, top_n_traceback));

    // test number of search results
    BOOST_REQUIRE_EQUAL(12u, results.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_scores.size());
    BOOST_REQUIRE_EQUAL(results.size(), expected_evalues.size());

    // for each result test score and E-value
    for (size_t i=0;i < results.size();i++) {
        CNcbiStrstream ss;
        ss << results[i].second;
        CSeq_align sa;
        ss >> MSerial_AsnBinary >> sa;
        double evalue;
        int score;
        BOOST_REQUIRE(sa.GetNamedScore(CSeq_align::eScore_EValue, evalue));
        BOOST_REQUIRE(sa.GetNamedScore(CSeq_align::eScore_Score, score));
        BOOST_CHECK_CLOSE( results[i].first, evalue, 0.0001 );
        BOOST_REQUIRE_EQUAL(score, expected_scores[i]);

        // Database size for SPARK is currently hard coded to 54105829280
        // in blast4spark.cpp. This is to represent full NR size. This test and
        // may fail when this changes.
        BOOST_REQUIRE_CLOSE(evalue, expected_evalues[i], 0.01);
    }

}


BOOST_AUTO_TEST_SUITE_END()
