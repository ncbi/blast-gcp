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
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.*;

public class Test_BLAST_LIB {

  private static final String LOGLEVEL = "INFO";
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
    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so", true);
    assertNotNull("should not be null", blaster);
  }
  /*
    @Test
    public void testNucleotideSearch() throws Exception {
      final String program = "blastn";

      final BC_DATABASE_SETTING dbset = new BC_DATABASE_SETTING();
      dbset.key = "nt";
      dbset.worker_location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";

      dbset.direct = true;
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
  */

  @Test
  public void testProteinSearch() throws Exception {
    final String queryfname = "data/prot_query.asn";
    final String program = "blastp";

    final BC_CHUNK_VALUES protdb = new BC_CHUNK_VALUES("data/prot_w_aux");
    protdb.files.add(new BC_NAME_SIZE("pax", new BigInteger("130")));
    protdb.files.add(new BC_NAME_SIZE("pin", new BigInteger("112")));
    protdb.files.add(new BC_NAME_SIZE("psq", new BigInteger("2434")));

    final Path cwd = Paths.get(System.getProperty("user.dir"), "../lib_builder/unit_test/");
    final Path testpath = cwd.normalize();
    System.out.println("Testdir is " + testpath);

    final String query = readFile(testpath + "/" + queryfname).replace("\n", "");

    final BC_DATABASE_SETTING dbset = new BC_DATABASE_SETTING();
    dbset.key = "nt";
    dbset.worker_location = testpath.toString();

    dbset.direct = true;
    final BC_DATABASE_RDD_ENTRY chunk = new BC_DATABASE_RDD_ENTRY(dbset, protdb);

    String params = "{";
    params += "\n\"version\": 1,";
    params += "\n \"RID\": \"" + "rid" + "\" ,";
    params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
    params += "\n";

    final BC_REQUEST requestobj = new BC_REQUEST();
    requestobj.query_seq = query;
    requestobj.params = "";
    requestobj.id = "rid";
    requestobj.db = params;
    requestobj.program = program;
    requestobj.params = params;

    if (!requestobj.valid()) {
      System.err.println("Warning (OK if test): BC_REQUEST not valid.");
    }
    requestobj.top_n_prelim = TOPN;
    requestobj.top_n_traceback = TOPN;

    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so", true);

    assertNotNull("shouldn't be null", blaster);

    final BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(chunk, requestobj, LOGLEVEL);

    assertEquals(hspl.length, 3);
    assertEquals(hspl[0].getOid(), 0x0);
    assertEquals(hspl[0].getMax_score(), 896);
    assertEquals(hspl[0].getHsp_blob().length, 40);
    assertEquals(hspl[1].getOid(), 0x4);
    assertEquals(hspl[1].getMax_score(), 296);
    assertEquals(hspl[1].getHsp_blob().length, 40);
    assertEquals(hspl[2].getOid(), 0x3);
    assertEquals(hspl[2].getMax_score(), 288);
    assertEquals(hspl[2].getHsp_blob().length, 40);
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(hspl[0].getHsp_blob()),
        "000000008003000000000000AE000000000000000E00000000000000AE000000000000000E000000");

    final BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, chunk, requestobj, LOGLEVEL);
    assertEquals(tbs.length, 3);
    assertEquals(tbs[0].top_n, 100);
    assertEquals(tbs[0].asn1_blob.length, 467);
    assertEquals(tbs[1].top_n, 100);
    assertEquals(tbs[1].asn1_blob.length, 552);
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(tbs[0].asn1_blob),
        "3080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A1800202038C0000000000003080A080A1801A0B626C6173745F73636F726500000000A180A1800202038C0000000000003080A080A1801A07655F76616C756500000000A180A080091600382E3732313730323133343233343337652D3131390000000000003080A080A1801A096269745F73636F726500000000A180A0800911003335342E3336393434373231333030310000000000003080A080A1801A096E756D5F6964656E7400000000A180A180020200AE0000000000003080A080A1801A16636F6D705F61646A7573746D656E745F6D6574686F6400000000A180A1800201020000000000003080A080A1801A0D6E756D5F706F7369746976657300000000A180A180020200AE0000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A08009040031303000000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A0771756572795F3100000000AB80020450F26321000000000000A380308002010002010000000000A4803080020200AE00000000A58030800A01000A0100000000000000000000000000");
  }

  @Test
  public void testNucleotideSearch() throws Exception {
    final String queryfname = "data/nucl_query.asn";
    final String program = "blastn";

    final BC_CHUNK_VALUES nucldb = new BC_CHUNK_VALUES("data/nucl_w_aux");
    nucldb.files.add(new BC_NAME_SIZE("nax", new BigInteger("136")));
    nucldb.files.add(new BC_NAME_SIZE("nin", new BigInteger("136")));
    nucldb.files.add(new BC_NAME_SIZE("nsq", new BigInteger("2418")));

    final Path cwd = Paths.get(System.getProperty("user.dir"), "../lib_builder/unit_test/");
    final Path testpath = cwd.normalize();
    System.out.println("Testdir is " + testpath);

    final String query = readFile(testpath + "/" + queryfname).replace("\n", "");

    final BC_DATABASE_SETTING dbset = new BC_DATABASE_SETTING();
    dbset.key = "nt";
    dbset.worker_location = testpath.toString();

    dbset.direct = true;
    final BC_DATABASE_RDD_ENTRY chunk = new BC_DATABASE_RDD_ENTRY(dbset, nucldb);

    String params = "{";
    params += "\n\"version\": 1,";
    params += "\n \"RID\": \"" + "rid" + "\" ,";
    params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
    params += "\n";

    final BC_REQUEST requestobj = new BC_REQUEST();
    requestobj.query_seq = query;
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

    final BLAST_LIB blaster = new BLAST_LIB("blastjni.so", true);

    assertNotNull("shouldn't be null", blaster);

    final BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(chunk, requestobj, LOGLEVEL);

    assertEquals(hspl.length, 5);
    assertEquals(hspl[0].getOid(), 0x0);
    assertEquals(hspl[0].getMax_score(), 2630);
    assertEquals(hspl[0].getHsp_blob().length, 40);
    assertEquals(hspl[1].getOid(), 0x1);
    assertEquals(hspl[1].getMax_score(), 2038);
    assertEquals(hspl[1].getHsp_blob().length, 120);
    assertEquals(hspl[2].getOid(), 0x2);
    assertEquals(hspl[2].getMax_score(), 1341);
    assertEquals(hspl[2].getHsp_blob().length, 160);
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(hspl[0].getHsp_blob()),
        "00000000460A000000000000460A0000010000009202000000000000460A00000100000092020000");

    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(hspl[1].getHsp_blob()),
        "01000000F6070000440200003A0A000001000000420400004101000037090000010000003F03000001000000AA00000000000000AA000000010000002B00000000000000AA000000010000002B0000000100000071000000A90000001A01000001000000C5000000D40000004501000001000000F0000000");

    final BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, chunk, requestobj, LOGLEVEL);
    assertEquals(tbs.length, 5);

    System.out.println(javax.xml.bind.DatatypeConverter.printHexBinary(tbs[1].asn1_blob));

    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(tbs[0].asn1_blob),
        "3080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A18002020A460000000000003080A080A1801A0B626C6173745F73636F726500000000A180A18002020A460000000000003080A080A1801A07655F76616C756500000000A180A080090200300000000000003080A080A1801A096269745F73636F726500000000A180A080091100343835372E38303838373938383233320000000000003080A080A1801A096E756D5F6964656E7400000000A180A18002020A460000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A08009040031303000000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A0771756572795F3100000000A9803080A1801A094E4D5F3137383237310000A38002010200000000000000000000A380308002010002010000000000A480308002020A4600000000A58030800A01010A0101000000000000000000000000");
    assertEquals(
        javax.xml.bind.DatatypeConverter.printHexBinary(tbs[1].asn1_blob),
        "3080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A180020207F60000000000003080A080A1801A0B626C6173745F73636F726500000000A180A180020207F60000000000003080A080A1801A07655F76616C756500000000A180A080090200300000000000003080A080A1801A096269745F73636F726500000000A180A080091000333736342E353932323835363938330000000000003080A080A1801A096E756D5F6964656E7400000000A180A180020207F60000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A08009110037372E3939303439343239363537373900000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A0771756572795F3100000000A9803080A1801A0C4E4D5F3030313332313238380000A38002010100000000000000000000A3803080020202440202014100000000A4803080020207F600000000A58030800A01010A01010000000000000000000000003080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A180020200AA0000000000003080A080A1801A0B626C6173745F73636F726500000000A180A180020200AA0000000000003080A080A1801A07655F76616C756500000000A180A080091500362E3436363631363837353335323737652D38310000000000003080A080A1801A096269745F73636F726500000000A180A0800911003331352E3035303733353133313135360000000000003080A080A1801A096E756D5F6964656E7400000000A180A180020200AA0000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A080091000362E3936333837383332363939363200000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A0771756572795F3100000000A9803080A1801A0C4E4D5F3030313332313238380000A38002010100000000000000000000A380308002010002010000000000A4803080020200AA00000000A58030800A01010A01010000000000000000000000003080A0800A01030000A1800201020000A28031803080A080A1801A0573636F726500000000A180A1800201710000000000003080A080A1801A0B626C6173745F73636F726500000000A180A1800201710000000000003080A080A1801A07655F76616C756500000000A180A080091500332E3133393038313036323632383234652D34390000000000003080A080A1801A096269745F73636F726500000000A180A0800911003230392E3739313730343934373839370000000000003080A080A1801A096E756D5F6964656E7400000000A180A1800201710000000000003080A080A1801A146873705F70657263656E745F636F76657261676500000000A180A080091100342E373936353737393436373638303600000000000000000000A380A1803080A0800201020000A1800201010000A2803080A080A1801A0771756572795F3100000000A9803080A1801A0C4E4D5F3030313332313238380000A38002010100000000000000000000A3803080020200A9020200D400000000A480308002017100000000A58030800A01010A0101000000000000000000000000");
  }
}
