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

import com.google.gson.Gson;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

final class BLAST_QUERY implements Serializable {
  // From JSON request
  private String protocol;
  private String RID;
  private String db_tag;
  private String db_selector;
  private int top_N_prelim;
  private int top_N_traceback;
  private String query_seq;
  private String query_url;
  private String program;
  private String blast_params;
  private String UserID;
  private Timestamp StartTime;
  // Partitioned
  private int partition_num;
  // HSPs from prelim_search
  private BLAST_HSP_LIST[] hspl;
  /*
  private int oid;
  private int max_score;
  private byte[] hsp_blob;
  */
  // Tracebacks from traceback
  private BLAST_TB_LIST[] tbl;
  /*
  private double evalue;
  private byte[] asn1_blob;
  */
  private String errorlist="";
  private long bench;

  public BLAST_QUERY() {
      errorlist="ctor";
      bench=System.currentTimeMillis();
  }

  // Copy Constructor
  public BLAST_QUERY(BLAST_QUERY in) {
    protocol = in.protocol;
    RID = in.RID;
    db_selector = in.db_selector;
    db_tag = in.db_tag;
    top_N_prelim = in.top_N_prelim;
    top_N_traceback = in.top_N_traceback;
    query_seq = in.query_seq;
    query_url = in.query_url;
    program = in.program;
    blast_params = in.blast_params;
    StartTime = in.StartTime;
    partition_num = in.partition_num;
    hspl = in.hspl;
    tbl = in.tbl;

    errorlist="copy ctor";
    bench=System.currentTimeMillis();
  }

  // Constructor from JSON string
  public BLAST_QUERY(String jsonString) {
    Logger logger = LogManager.getLogger(BLAST_QUERY.class);

    JSONObject json = new JSONObject(jsonString);
    try {
      protocol = json.optString("protocol", "");
      if (protocol.equals("1.0")) {
        Gson gson = new Gson();
        BLAST_QUERY in = gson.fromJson(jsonString, BLAST_QUERY.class);
        in.db_selector = in.db_tag.substring(0, 2);
        protocol = in.protocol;
        RID = in.RID;
        db_selector = in.db_selector;
        db_tag = in.db_tag;
        top_N_prelim = in.top_N_prelim;
        top_N_traceback = in.top_N_traceback;
        query_seq = in.query_seq;
        query_url = in.query_url;
        program = in.program;
        blast_params = in.blast_params;
        StartTime = in.StartTime;
        partition_num = in.partition_num;
        hspl = in.hspl;
        tbl = in.tbl;

        errorlist="json ctor";
        bench=System.currentTimeMillis();

        /*
        rid = json.optString("RID", "");
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
    }
  }

  String getProtocol() {

    return protocol;
  }
  /*
  void setProtocol(final String protocol) {
  this.protocol = protocol;
  }
  */

  String getRid() {
    return RID;
  }

  void setRid(final String RID) {
    this.RID = RID;
  }

  String getDb_tag() {
    return db_tag;
  }

  void setDb_tag(final String db_tag) {
    this.db_tag = db_tag;
  }

  String getDb_selector() {
    return db_selector;
  }

  void setDb_selector(final String db_selector) {
    this.db_selector = db_selector;
  }

  int getTop_N_prelim() {
    return top_N_prelim;
  }
  /*
  void setTop_N_prelim(final int top_N_prelim) {
  this.top_N_prelim = top_N_prelim;
  }
  */

  int getTop_N_traceback() {
    return top_N_traceback;
  }
  /*
  void setTop_N_traceback(final int top_N_traceback) {
  this.top_N_traceback = top_N_traceback;
  }
  */
  String getQuery_seq() {
    return query_seq;
  }

  void setQuery_seq(final String query_seq) {
    this.query_seq = query_seq;
  }

  String getQuery_url() {
    return query_url;
  }
  /*
  void setQuery_url(final String query_url) {
  this.query_url = query_url;
  }
  */
  String getProgram() {
    return program;
  }
  /*
  void setProgram(final String program) {
  this.program = program;
  }
  */
  String getBlast_params() {
    return blast_params;
  }
  /*
  void setBlast_params(final String blast_params) {
  this.blast_params = blast_params;
  }
  */
  Timestamp getStartTime() {
    return StartTime;
  }
  /*
  void setStartTime(final Timestamp StartTime) {
  this.StartTime = StartTime;
  }
  */
  BLAST_HSP_LIST[] getHspl() {
    return hspl;
  }

  void setHspl(BLAST_HSP_LIST[] hspl) {
    this.hspl = hspl;
  }

  BLAST_TB_LIST[] getTbl() {
    return tbl;
  }

  void setTbl(BLAST_TB_LIST[] tbl) {
    this.tbl = tbl;
  }

  void setPartition_num(int partition_num) {
    this.partition_num = partition_num;
  }

  // FIX: Serdes with ProtocolBuffer?
  public String toJson() {
    return toString();
  }

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
