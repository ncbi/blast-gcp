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

int main( int argc, char * argv[] )
{
    if ( argc != 2 )
    {
        std::cerr << "Usage: " << argv[0] << "query.json\n";
        return 1;
    }

    const char * jsonfname = argv[1];
    std::ifstream jsonfile( jsonfname );
    std::stringstream buffer;
    buffer << jsonfile.rdbuf();
    std::string jsontext = buffer.str();

    json j = json::parse( jsontext );

    std::cout << j.dump( 4 ) << std::endl;

    std::string query=j["query_seq"];
    std::string db_spec=j["db_tag"];
    std::string program=j["program"];
    std::string params=j["blast_params"];
    int top_n_prelim=j["top_N_prelim"];
    int top_n_traceback=j["top_N_traceback"];

    std::string db=std::string(db_spec,0,2);
    std::string location="/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";
    if (db=="nr")
    {
        db=location + "nr_50M.14";
    }
    else if (db=="nt")
    {
        db=location + "nt_50M.14";
    }
    else
    {
        std::cerr << "Unknown db:" << db << "\n";
        return 2;
    }

    auto alignments=searchandtb(query, db, program, params, top_n_prelim, top_n_traceback );
}

