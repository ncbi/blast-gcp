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

import java.io.Serializable;
import java.util.*;

class PART implements Serializable {
  private String db_select;
  private int part_num;

  public PART() {}

  String getDb_select() {

    return db_select;
  }

  void setDb_select(final String db_select) {
    this.db_select = db_select;
  }

  int getPart_num() {
    return part_num;
  }

  void setPart_num(final int part_num) {
    this.part_num = part_num;
  }

  @Override
  public String toString() {
    return String.format("%s,%d", db_select, part_num);
  }
}
