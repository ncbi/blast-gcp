/*
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
 */

package gov.nih.nlm.ncbi.blastjni;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import org.apache.log4j.Level;
import org.json.JSONArray;
import org.json.JSONObject;

public class BLAST_LIB {
    public BLAST_LIB(final String iblast_json) {
        blast_json = iblast_json;
        processID = ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0];
        logLevel = Level.INFO;
    }

    private static String blast_json;
    private static Level logLevel;
    private static String processID;

    // We can't rely on log4j.properties to filter, instead we'll look at
    // logLevel
    private void log(final String level, final String msg) {
        try {
            long threadId = Thread.currentThread().getId();

            final String newmsg = "BLASTJNI (" + BLAST_LIB.processID + "/" + threadId + ") " + msg;
            System.err.println(level + " : " + newmsg
                    .replace("\n"," ")
                    .replace("\r"," "));
        } catch (Throwable threx) {
            {
                System.err.println("ERROR Log throw");
                if (msg != null) System.err.println(msg);
            }
        }
    }

    final BLAST_HSP_LIST[] jni_prelim_search(
            final BLAST_DATABASE_PART part, final BLAST_REQUEST req, final String pslogLevel)
            throws Exception {

            BLAST_LIB.logLevel = Level.toLevel(pslogLevel);

            log("INFO", "Java jni_prelim_search called with");
            log("INFO", "  query_seq : " + req.query_seq);
            log("INFO", "  db_spec   : " + part.db_spec);
            log("INFO", "  program   : " + req.program);
            log("INFO", "  topn      : " + req.top_n_prelim);

            if (req.query_seq.contains("\n")) {
                log("WARN", "Query contains newline, which may crash Blast library");
            }

            String query = req.query_seq;

            JSONObject jsonout = new JSONObject();
            jsonout.put("protocol", "prelim-json-1.0");
            jsonout.put("query_seq", query);
            jsonout.put("db_location", part.db_spec);
            jsonout.put("program", req.program);
            jsonout.put("blast_params", req.params);
            jsonout.put("top_N_prelim", req.top_n_prelim);

            File tempFile = File.createTempFile("blast-prelim-", ".json");
            String tempFileName = tempFile.getCanonicalPath();
            log("INFO", "JSON file will be " + tempFileName);
            try (PrintWriter out = new PrintWriter(tempFileName)) {
                out.println(jsonout.toString(2));
            }
            // tempFile.deleteOnExit();

            long starttime = System.currentTimeMillis();

            ProcessBuilder builder = new ProcessBuilder(blast_json, "prelim_search", tempFileName);
            builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
            Process process = builder.start();

            StringBuilder pout = new StringBuilder();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {

                reader.lines().forEach(line -> pout.append(line + "\n"));
                    }

            process.waitFor();
            int exitval=process.exitValue();
            if (exitval!=0)
            {
                log("ERROR", String.format("%s returned non-zero exit value %d", blast_json, exitval));
            }

            long finishtime = System.currentTimeMillis();
            if (finishtime - starttime > 50000)
                log(
                        "WARN",
                        "LONG jni_prelim_search returned in "
                        + (finishtime - starttime)
                        + " ms."
                        + " for query="
                        + query
                        + " ,db_spec="
                        + part.db_spec);
            log(
                    "INFO",
                    "jni_prelim_search returned in "
                    + (finishtime - starttime)
                    + " ms."
                    + " for query="
                    + query
                    + " ,db_spec="
                    + part.db_spec);

            // log("INFO", "Returned string was: " + pout.toString());
            JSONObject jsonin = new JSONObject(pout.toString());
            //log("INFO", "Returned JSON was: " + jsonin.toString(2));
            JSONArray jhsplarr = jsonin.getJSONArray("blast_hsp_list");
            ArrayList<BLAST_HSP_LIST> alhsp = new ArrayList<>();

            for (int i = 0; i != jhsplarr.length(); ++i) {
                JSONObject j = jhsplarr.getJSONObject(i);
                int max_score = j.getInt("max_score");
                int oid = j.getInt("oid");
                JSONArray jblob = j.getJSONArray("hsp_blob");
                byte[] blob = new byte[jblob.length()];
                for (int b = 0; b != jblob.length(); ++b) {
                    blob[b] = (byte) jblob.getInt(b);
                }
                log("INFO", String.format("idx %d, oid=%d, max_score=%d", i, oid, max_score));
                BLAST_HSP_LIST hspl = new BLAST_HSP_LIST(oid, max_score, blob);
                alhsp.add(hspl);
            }

            BLAST_HSP_LIST[] ret = alhsp.toArray(new BLAST_HSP_LIST[0]);

            log("INFO", "jni_prelim_search returned " + ret.length + " HSP_LISTs:");

            return ret;
            }

    final BLAST_TB_LIST[] jni_traceback(
            final BLAST_HSP_LIST[] hspl,
            final BLAST_DATABASE_PART part,
            final BLAST_REQUEST req,
            final String tblogLevel)
            throws Exception {

            BLAST_LIB.logLevel = Level.toLevel(tblogLevel);
            log("INFO", "Java jni_traceback called with");
            log("INFO", "  query_seq : " + req.query_seq);
            log("INFO", "  db_spec   : " + part.db_spec);
            log("INFO", "  hspls     : " + Integer.toString(hspl.length));

            String query = req.query_seq;
            long starttime = System.currentTimeMillis();

            JSONObject jsonout = new JSONObject();
            jsonout.put("protocol", "traceback-json-1.0");
            jsonout.put("query_seq", query);
            jsonout.put("db_location", part.db_spec);
            jsonout.put("program", req.program);
            jsonout.put("blast_params", req.params);
            jsonout.put("top_N_traceback", req.top_n_traceback);

            JSONArray jhsplarr = new JSONArray();
            for (int i = 0; i != hspl.length; ++i) {
                JSONObject j = new JSONObject();
                j.put("max_score", hspl[i].max_score);
                j.put("oid", hspl[i].oid);
                JSONArray jblob = new JSONArray();
                for (int b = 0; b != hspl[i].hsp_blob.length; ++b) {
                    jblob.put(b, hspl[i].hsp_blob[b]);
                }
                j.put("hsp_blob", jblob);
                jhsplarr.put(i, j);
            }

            jsonout.put("blast_hsp_list", jhsplarr);

            File tempFile = File.createTempFile("blast-traceback-", ".json");
            String tempFileName = tempFile.getCanonicalPath();
            log("INFO", "JSON file will be " + tempFileName);
            try (PrintWriter out = new PrintWriter(tempFileName)) {
                out.println(jsonout.toString(2));
            }
            // tempFile.deleteOnExit();

            ProcessBuilder builder = new ProcessBuilder(blast_json, "traceback", tempFileName);
            builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
            Process process = builder.start();

            StringBuilder pout = new StringBuilder();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {

                reader.lines().forEach(line -> pout.append(line + "\n"));
                    }

            process.waitFor();
            long finishtime = System.currentTimeMillis();
            if (finishtime - starttime > 50000)
                log(
                        "WARN",
                        "LONG jni_treacback returned in "
                        + (finishtime - starttime)
                        + " ms."
                        + " for query="
                        + query
                        + " ,db_spec="
                        + part.db_spec);
            log(
                    "INFO",
                    "jni_traceback returned in "
                    + (finishtime - starttime)
                    + " ms."
                    + " for query="
                    + query
                    + " ,db_spec="
                    + part.db_spec);

            // log("INFO", "Returned string was: " + pout.toString());
            JSONObject jsonin = new JSONObject(pout.toString());
            //log("INFO", "Returned JSON was: " + jsonin.toString(2));
            JSONArray jtblarr = jsonin.getJSONArray("blast_tb_list");
            ArrayList<BLAST_TB_LIST> altbl = new ArrayList<>();

            for (int i = 0; i != jtblarr.length(); ++i) {
                JSONObject j = jtblarr.getJSONObject(i);
                double evalue = j.getDouble("evalue");
                int oid = j.getInt("oid");
                JSONArray jblob = j.getJSONArray("asn1_blob");
                byte[] blob = new byte[jblob.length()];
                for (int b = 0; b != jblob.length(); ++b) {
                    blob[b] = (byte) jblob.getInt(b);
                }
                log("INFO", String.format("idx %d, evalue=%f", i, evalue));
                BLAST_TB_LIST tbl = new BLAST_TB_LIST(oid, evalue, blob);
                tbl.top_n = req.top_n_traceback;
                altbl.add(tbl);
            }

            BLAST_TB_LIST[] ret = altbl.toArray(new BLAST_TB_LIST[0]);

            log("INFO", "jni_traceback returned " + ret.length + " TB_LISTs.");

            return ret;
            }
}
