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
 * ===========================================================================
 *
 */

package gov.nih.nlm.ncbi.blastjni;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.io.Serializable;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

final class BLAST_QUERY implements Serializable {
  // From JSON request
  String protocol;

  @SerializedName("RID")
  String rid;

  int[] volumes; // Future
  String db_tag;
  String db_selector;

  @SerializedName("top_N_prelim")
  int top_n_prelim;

  @SerializedName("top_N_traceback")
  int top_n_traceback;

  String query_seq;
  String query_url;
  String program;
  String blast_params;

  @SerializedName("UserID")
  String userid;

  @SerializedName("StartTime")
  String starttime;
  // Partitioned
  int partition_num;
  // HSPs from prelim_search
  BLAST_HSP_LIST[] hspl;
  // Tracebacks from traceback
  BLAST_TB_LIST[] tbl;
  // Debugging/tracing aids
  int prelim_partition_num;
  String errorlist;
  long bench;

  BLAST_QUERY() {
    errorlist = "ctor";
    bench = System.currentTimeMillis();
  }

  // Copy Constructor
  BLAST_QUERY(BLAST_QUERY in) {
    protocol = in.protocol;
    rid = in.rid;
    db_selector = in.db_selector;
    db_tag = in.db_tag;
    top_n_prelim = in.top_n_prelim;
    top_n_traceback = in.top_n_traceback;
    query_seq = in.query_seq;
    query_url = in.query_url;
    program = in.program;
    blast_params = in.blast_params;
    starttime = in.starttime;
    partition_num = in.partition_num;
    hspl = in.hspl;
    tbl = in.tbl;

    errorlist = "copy ctor";
    bench = System.currentTimeMillis();
    prelim_partition_num = in.prelim_partition_num;
  }

  // Constructor from JSON string
  BLAST_QUERY(final String jsonString) {
    Logger logger = LogManager.getLogger(BLAST_QUERY.class);

    JSONObject json = new JSONObject(jsonString);
    try {
      protocol = json.optString("protocol", "");
      if ("1.0".equals(protocol)) {
        Gson gson = new Gson();
        BLAST_QUERY in = gson.fromJson(jsonString, BLAST_QUERY.class);
        in.db_selector = in.db_tag.substring(0, 2);
        protocol = in.protocol;
        rid = in.rid;
        db_selector = in.db_selector;
        db_tag = in.db_tag;
        top_n_prelim = in.top_n_prelim;
        top_n_traceback = in.top_n_traceback;
        query_seq = in.query_seq;
        query_url = in.query_url;
        program = in.program;
        blast_params = in.blast_params;
        starttime = in.starttime;
        partition_num = in.partition_num;
        hspl = in.hspl;
        tbl = in.tbl;

        prelim_partition_num = in.prelim_partition_num;
        errorlist = "json ctor";
        bench = System.currentTimeMillis();

      } else {
        logger.log(Level.ERROR, "Unknown JSON protocol: " + protocol);
      }

    } catch (Exception e) {
      logger.log(Level.ERROR, "JSON parsing error: " + e);
    }
  }

  public String toJson() {
    Logger logger = LogManager.getLogger(BLAST_QUERY.class);
    Kryo kryo = new Kryo();
    Output output = new Output(32768, -1);
    kryo.writeObject(output, this);
    output.close();
    byte[] k = output.getBuffer();
    int pos = output.position();

    String t = toString();
    logger.log(
        Level.INFO,
        String.format("Info: Kryo size would be %d %d vs. JSON's %d", k.length, pos, t.length()));


    return t;
  }

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
