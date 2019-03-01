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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.*;

public class BLASTLIBTest {

  private static final String LOGLEVEL = "DEBUG";
  private static final int TOPN = 100;

  private String readFile(final String filename) throws IOException {
    return new String(Files.readAllBytes(Paths.get(filename)), "UTF-8");
  }

  // TODO: Bad params test

  @Test
  public void testBadLoadLibrary() {
    System.out.println("Libary path is " + System.getProperty("java.library.path"));
    // BLAST_LIB blaster = null;
    /*
    boolean caught = false;
    try {
      blaster = new BLAST_LIB("notalibrary.so");
    } catch (NullPointerException npe) {
      caught = true;
      System.out.println("Caught npe");
    } catch (ExceptionInInitializerError eiie) {
      System.out.println("Caught eiie");
      caught = true;
    }
    //assertNull("should be null", blaster);
    assertTrue(caught);
    //throw new NullPointerException();
    */
  }

  @Test
  public void testLoadLibrary() {
    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so");
    assertNotNull("should not be null", blaster);
  }

  @Test
  public void testNucleotideSearch() throws Exception {
    final String program = "blastn";

    final BC_DATABASE_SETTING dbset = new BC_DATABASE_SETTING();
    dbset.key = "nt";
    dbset.worker_location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";

    dbset.flat_layout = true;
    final BC_DATABASE_RDD_ENTRY chunk = new BC_DATABASE_RDD_ENTRY(dbset, "nt_50M.14");

    String params = "{";
    params += "\n\"version\": 1,";
    params += "\n \"RID\": \"" + "rid" + "\" ,";
    params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
    params += "\n";

    final BC_REQUEST requestobj = new BC_REQUEST();
    requestobj.query_seq = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
    requestobj.params = "";
    requestobj.id = "rid";
    requestobj.db = params;
    requestobj.program = program;
    requestobj.params = params;

    if (!requestobj.valid()) {
      System.err.println("Warning: BC_REQUEST not valid:\t" + requestobj);
    }
    requestobj.top_n_prelim = TOPN;
    requestobj.top_n_traceback = TOPN;

    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so");

    assertNotNull("shouldn't be null", blaster);

    final BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(chunk, requestobj, LOGLEVEL);
    assertEquals(hspl.length, 2);
    assertEquals(hspl[0].getOid(), 0x3ff6);
    assertEquals(hspl[0].getMax_score(), 61);
    assertEquals(hspl[0].getHsp_blob().length, 40);
    assertEquals(hspl[1].getOid(), 0x4a73);
    assertEquals(hspl[1].getMax_score(), 70);
    assertEquals(hspl[1].getHsp_blob().length, 40);
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(hspl[0].getHsp_blob()),
        "F63F00003D0000000000000046000000010000002E000000490100008F0100000100000077010000");

    final BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, chunk, requestobj, LOGLEVEL);
    assertEquals(tbs.length, 2);
    assertEquals(tbs[0].top_n, 100);
    assertEquals(tbs[0].asn1_blob.length, 375);
    assertEquals(tbs[1].top_n, 100);
    assertEquals(tbs[1].asn1_blob.length, 375);
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(tbs[0].asn1_blob),
        "3080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A1800201460000000000003080A080A1801A0B626C6173745F73636F726500000000A180A1800201460000000000003080A080A1801A07655F76616C756500000000A180A080091500332E3738343931313333343030383733652D32370000000000003080A080A1801A096269745F73636F726500000000A180A0800911003133302E3338353736393839373336390000000000003080A080A1801A096E756D5F6964656E7400000000A180A1800201460000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A08009040031303000000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A05717565727900000000AB80020403B3ED0E000000000000A38030800201000202014E00000000A480308002014600000000A58030800A01010A0101000000000000000000000000");
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(tbs[1].asn1_blob),
        "3080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A18002013D0000000000003080A080A1801A0B626C6173745F73636F726500000000A180A18002013D0000000000003080A080A1801A07655F76616C756500000000A180A080091500332E3831313738323736313137383139652D32320000000000003080A080A1801A096269745F73636F726500000000A180A0800911003131332E3736353932333032363332380000000000003080A080A1801A096E756D5F6964656E7400000000A180A1800201430000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A08009040031303000000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A05717565727900000000AB80020403B1ED3B000000000000A38030800201000202014900000000A480308002014600000000A58030800A01010A0101000000000000000000000000");
  }
  /*
  public void testProteinSearch() throws Exception {
    final String queryfname = "prot_query.asn";
    final String protdb = "prot_w_aux";
    final String program = "blastn";

    final Path cwd = Paths.get(System.getProperty("user.dir"), "../lib_builder/unit_test/data/");
    final Path testpath = cwd.normalize();
    System.out.println("Testdir is " + testpath);

    final String query = readFile(testpath + "/" + queryfname).replace("\n", "");

    final BC_DATABASE_SETTING dbset = new BC_DATABASE_SETTING();
    dbset.key = "nt";
    dbset.worker_location = testpath + "/" + protdb;
    dbset.worker_location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";

    dbset.flat_layout = true;
    final BC_DATABASE_RDD_ENTRY chunk = new BC_DATABASE_RDD_ENTRY(dbset, "nt_50M.14");

    String params = "{";
    params += "\n\"version\": 1,";
    params += "\n \"RID\": \"" + "rid" + "\" ,";
    params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
    params += "\n";

    final BC_REQUEST requestobj = new BC_REQUEST();
    requestobj.query_seq = query;
    requestobj.query_seq = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
    requestobj.params = "";
    requestobj.id = "rid";
    requestobj.db = params;
    requestobj.program = program;
    requestobj.params = params;

    if (!requestobj.valid()) {
      System.err.println("Warning: BC_REQUEST not valid:\t" + requestobj);
    }
    requestobj.top_n_prelim = TOPN;
    requestobj.top_n_traceback = TOPN;

    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so");

    assertNotNull("shouldn't be null", blaster);

    final BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(chunk, requestobj, LOGLEVEL);
  }
  */
}
