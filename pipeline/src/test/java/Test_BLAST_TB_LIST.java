package gov.nih.nlm.ncbi.blastjni;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.junit.*;

public class Test_BLAST_TB_LIST {
  @Test
  public void testGetters() {
    final byte[] blob1 = new byte[] {};
    final byte[] blob2 = new byte[] {0x01};
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 1, blob1);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1, blob2);

    assertTrue(test1.isEmpty());
    assertFalse(test2.isEmpty());
  }

  @Test
  public void testCompares() {
    final byte[] blob = new byte[] {};
    // These should be listed in desired sorting order
    final BLAST_TB_LIST test5 = new BLAST_TB_LIST(1, -2, blob);
    final BLAST_TB_LIST test3 = new BLAST_TB_LIST(1, 1, blob);
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 1, blob);
    final BLAST_TB_LIST test6 = new BLAST_TB_LIST(0, 1, blob);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1, blob);
    final BLAST_TB_LIST test10 = new BLAST_TB_LIST(1, 1, blob);
    final BLAST_TB_LIST test4 = new BLAST_TB_LIST(1, 2, blob);
    final BLAST_TB_LIST test7 = new BLAST_TB_LIST(1, 3, blob);

    assertEquals("reflexive", test1.compareTo(test1), 0);
    assertEquals("reflexive", test2.compareTo(test2), 0);
    assertEquals("reflexive", test3.compareTo(test3), 0);
    assertEquals("reflexive", test4.compareTo(test4), 0);
    assertEquals("reflexive", test5.compareTo(test5), 0);
    assertEquals("reflexive", test6.compareTo(test6), 0);
    assertEquals("reflexive", test7.compareTo(test7), 0);
    assertEquals("reflexive", test10.compareTo(test10), 0);

    assertEquals(test5.compareTo(test3), 1);
    assertEquals(test3.compareTo(test5), -1);

    assertEquals("symmetric", test1.compareTo(test3), 0);
    assertEquals("symmetric", test2.compareTo(test3), 0);

    assertEquals(test1.compareTo(test4), 1);
    assertEquals(test4.compareTo(test1), -1);

    assertEquals(test1.compareTo(test5), -1);
    assertEquals(test5.compareTo(test1), 1);

    assertEquals(test4.compareTo(test7), 1);
    assertEquals(test7.compareTo(test4), -1);

    assertEquals("transitive", test5.compareTo(test7), 1);
    assertEquals("transitive", test7.compareTo(test5), -1);

    // 6<=2<=10
    assertEquals("transitive", test2.compareTo(test6), 0);
    assertEquals("transitive", test10.compareTo(test2), 0);
    assertEquals("transitive", test10.compareTo(test6), 0);
  }


  @Test
  public void testSort() {
    final byte[] blob = new byte[] {};
    // These should be listed in desired sorting order
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, -2, blob);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1, blob);

    ArrayList<BLAST_TB_LIST> l = new ArrayList<>();
    l.add(test1);
    l.add(test2);
    Collections.sort(l);
    assertTrue(l.get(0).evalue > l.get(1).evalue);
  }


  @Test
  public void testSort2() {
    final byte[] blob = new byte[] {};

    ArrayList<BLAST_TB_LIST> l = new ArrayList<>();
    Random rng = new Random();
    BLAST_TB_LIST test;
    for (int i = 0; i != 10000; ++i) {
      final int oid = rng.nextInt(5) - 2;
      final int evalue = rng.nextInt(50000);
      test = new BLAST_TB_LIST(oid, evalue, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid, evalue, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid + 1, evalue, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid, evalue, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid, evalue, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid, evalue + 1, blob);
      l.add(test);
      test = new BLAST_TB_LIST(oid + 1, evalue + 1, blob);
      l.add(test);
    }

    test = new BLAST_TB_LIST(0, 0, blob);
    l.add(test);
    l.add(test);
    test = new BLAST_TB_LIST(0, 0, blob);
    l.add(test);
    test = new BLAST_TB_LIST(-1, 0, blob);
    l.add(test);
    test = new BLAST_TB_LIST(1, 0, blob);
    l.add(test);

    for (int i=0; i!=2; ++i) {
        //test = new BLAST_TB_LIST(1, Double.NaN, blob);
        //l.add(test);
        //test = new BLAST_TB_LIST(1, Double.NEGATIVE_INFINITY, blob);
        l.add(test);
//        test = new BLAST_TB_LIST(1, Integer.POSITIVE_INFINITY, blob);
        l.add(test);
        test = new BLAST_TB_LIST(1, Integer.MIN_VALUE, blob);
        l.add(test);
    }

    // Extracted from REQ_AFFAUTCT014.asn1.unsorted
    test = new BLAST_TB_LIST(100, 9863, blob);
    l.add(test);
    l.add(test);
    test = new BLAST_TB_LIST(100, 6079, blob);
    l.add(test);
    l.add(test);

    Collections.sort(l);
    int prev_evalue = -99999;
    int prev_oid = -99999;
    for (BLAST_TB_LIST x : l) {
        final String why = String.format("%d %d  <=> %d %d", x.oid, x.evalue, prev_oid, prev_evalue);
      /*
      if (x.evalue - prev_evalue < BLAST_TB_LIST.epsilon) assertTrue(why, x.oid >= prev_oid);
      else
      */
//      assertTrue(why, x.evalue >= prev_evalue);

      prev_evalue = x.evalue;
      prev_oid = x.oid;
    }
    assertTrue(l.get(0).evalue <= l.get(1).evalue);
  }
}
