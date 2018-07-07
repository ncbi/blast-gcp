/*===========================================================================
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
 *===========================================================================
 *
 */

package gov.nih.nlm.ncbi.blastjni;

import com.google.gson.Gson;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.apache.spark.sql.*;
import org.json.JSONObject;

public final class BLAST_TEST implements Serializable {

    public static void main(final String[] args) throws Exception {
        final String location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";

        for (int arg = 0; arg != args.length; ++arg) {
            final String jsonfile = args[arg];

            final String jsonstr = new String(Files.readAllBytes(Paths.get(jsonfile)));

            BLAST_LIB blaster = new BLAST_LIB("");
            if (blaster == null) {
                System.out.println("NULL blaster library");
            }

            JSONObject json = new JSONObject(jsonstr);
            try {
                final String protocol = json.optString("protocol", "");
                if ("1.0".equals(protocol)) {
                    final String query_seq = json.getString("query_seq");
                    final String blast_params = json.getString("blast_params");
                    final String rid = json.getString("RID");
                    final String program = json.getString("program");
                    final String db_tag = json.getString("db_tag");
                    final int top_n_prelim = json.getInt("top_N_prelim");
                    final int top_n_traceback = json.getInt("top_N_traceback");

                    System.out.println(String.format("RID %s, db_tag %s", rid, db_tag));

                    final String db = db_tag.substring(0, 6);
                    BLAST_REQUEST requestobj = new BLAST_REQUEST();
                    requestobj.id = rid;
                    requestobj.query_seq = query_seq;
                    requestobj.params = blast_params;
                    requestobj.db = db;
                    requestobj.top_n_prelim = top_n_prelim;
                    requestobj.top_n_traceback = top_n_traceback;
                    requestobj.program = program;
                    System.out.println(requestobj.toString());
                    CONF_VOLUME cv = new CONF_VOLUME(db, "", "");
                    BLAST_DATABASE_PART partitionobj = new BLAST_DATABASE_PART(613, cv, location);
                    //partitionobj.db_spec=location+db+"456";
                    partitionobj.db_spec=String.format("%s/%s.%d", location, db, 613);
                    System.out.println(partitionobj.toString());

                    BLAST_HSP_LIST[] search_res =
                        blaster.jni_prelim_search(partitionobj, requestobj, "DEBUG");

                    if (search_res != null) {
                        System.out.println(" prelim_search returned " + search_res.length + " HSP lists:");
                        Gson gson = new Gson();
                        String hspjson = gson.toJson(search_res);
                        System.out.println("RID " + rid + " prelim_search JSON: " + hspjson);
                    } else {
                        System.out.println("NULL search_res");
                    }

                    BLAST_TB_LIST[] tb_res =
                        blaster.jni_traceback(search_res, partitionobj, requestobj, "DEBUG");

                    if (tb_res != null) {
                        Gson gson = new Gson();
                        String tbjson = gson.toJson(tb_res);
                        System.out.println("RID " + rid + " traceback JSON:" + tbjson);
                    } else {
                        System.out.println("NULL asn1");
                    }
                } else {
                    System.out.println("Unknown JSON protocol: " + protocol);
                }
            } catch (Exception e) {
                System.out.println("JSON parsing error: " + e);
            }
        }
    }
}
