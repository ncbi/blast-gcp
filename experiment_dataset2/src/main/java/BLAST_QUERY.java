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
  public String protocol;

  @SerializedName("RID")
  public String rid;

  public int[] volumes; // Future
  public String db_tag;
  public String db_selector;

  @SerializedName("top_N_prelim")
  public int top_n_prelim;

  @SerializedName("top_N_traceback")
  public int top_n_traceback;

  public String query_seq;
  public String query_url;
  public String program;
  public String blast_params;

  @SerializedName("UserID")
  public String userid;

  @SerializedName("StartTime")
  public String starttime;
  // Partitioned
  public int partition_num;
  // HSPs from prelim_search
  public BLAST_HSP_LIST[] hspl;
  // Tracebacks from traceback
  public BLAST_TB_LIST[] tbl;
  // Debugging/tracing aids
  public int prelim_partition_num;
  public String errorlist;
  public long bench;

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

        /*
        rid = json.optString("rid", "");
        db_tag = json.optString("db_tag", "");
        db_selector = db_tag.substring(0, 2); // json.optString("db_selector", "");
        top_N_prelim = json.optInt("top_N_prelim", 0);
        top_N_traceback = json.optInt("top_N_traceback", 0);
        query_seq = json.optString("query_seq", "");
        query_url = json.optString("query_url", "");
        program = json.optString("program", "");
        blast_params = json.optString("blast_params", "");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date parsedDate = dateFormat.parse(json.optString("StartTime", "1980-01-01T01:00:00.0"));
        StartTime = new java.sql.Timestamp(parsedDate.getTime());

        oid = json.optInt("oid", 0);
        partition_num = json.optInt("partition_num", 0);
        max_score = json.optInt("max_score", 0);
        hsp_blob = Base64.getDecoder().decode(json.optString("hsp_blob", ""));
        evalue = json.optDouble("evalue");
        asn1_blob = Base64.getDecoder().decode(json.optString("asn1_blob", ""));
        */
      } else {
        logger.log(Level.ERROR, "Unknown JSON protocol: " + protocol);
      }

    } catch (Exception e) {
      logger.log(Level.ERROR, "JSON parsing error: " + e);
    }
  }

  // FIX: Serdes with ProtocolBuffer? Kryo?
  public String toJson() {
    Logger logger = LogManager.getLogger(BLAST_QUERY.class);
    Kryo kryo = new Kryo();
    Output output = new Output(1000, -1);
    kryo.writeObject(output, this);
    output.close();
    byte[] k = output.getBuffer();
    logger.log(Level.INFO, String.format("Info: Kryo size would be %d", k.length));

    return toString();
  }

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
