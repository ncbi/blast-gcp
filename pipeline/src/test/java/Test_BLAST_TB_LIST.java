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
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 1, 0, blob1);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1, 0, blob2);

    assertTrue(test1.isEmpty());
    assertFalse(test2.isEmpty());
  }

  @Test
  public void testCompares() {
    final byte[] blob = new byte[] {};
    final int evalue = 500000;
    final int score = 1234;
    final int seqid = 5;

    // These should be listed in desired sorting order
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(evalue, score, seqid, blob);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(evalue, score, seqid, blob);
    final BLAST_TB_LIST test3
        = new BLAST_TB_LIST(evalue - 1, score + 1, seqid, blob);
    final BLAST_TB_LIST test4
        = new BLAST_TB_LIST(evalue - 1, score + 1, seqid, blob);
    final BLAST_TB_LIST test5
        = new BLAST_TB_LIST(evalue - 1, score, seqid, blob);
    final BLAST_TB_LIST test6
        = new BLAST_TB_LIST(evalue - 1, score - 1 , 1, blob);
    final BLAST_TB_LIST test7
        = new BLAST_TB_LIST(evalue - 1, score - 1, seqid - 1, blob);
    final BLAST_TB_LIST test8
        = new BLAST_TB_LIST(evalue - 2, score + 2, seqid + 2, blob);

    assertEquals("reflexive", test1.compareTo(test1), 0);
    assertEquals("reflexive", test2.compareTo(test2), 0);
    assertEquals("reflexive", test3.compareTo(test3), 0);
    assertEquals("reflexive", test4.compareTo(test4), 0);
    assertEquals("reflexive", test5.compareTo(test5), 0);
    assertEquals("reflexive", test6.compareTo(test6), 0);
    assertEquals("reflexive", test7.compareTo(test7), 0);
    assertEquals("reflexive", test8.compareTo(test8), 0);

    assertEquals("symmetric", test1.compareTo(test2), 0);
    assertEquals("symmetric", test2.compareTo(test1), test1.compareTo(test2));

    assertEquals(test1.compareTo(test3), -1);
    assertEquals(test3.compareTo(test1), 1);

    assertEquals(test1.compareTo(test5), -1);
    assertEquals(test5.compareTo(test1), 1);

    assertEquals(test4.compareTo(test7), -1);
    assertEquals(test7.compareTo(test4), 1);

    assertEquals(test7.compareTo(test8), -1);
    assertEquals(test8.compareTo(test7), 1);

    assertEquals("transitive", test5.compareTo(test7), -1);
    assertEquals("transitive", test7.compareTo(test5), 1);

    // 1 > 3 > 5
    assertEquals("transitive", test1.compareTo(test3), -1);
    assertEquals("transitive", test3.compareTo(test5), -1);
    assertEquals("transitive", test1.compareTo(test5), -1);
  }


  @Test
  public void testSort() {
    final byte[] blob = new byte[] {};
    // These should be listed in desired sorting order
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 0, 0, blob);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(0, 1, 0, blob);
    final BLAST_TB_LIST test3 = new BLAST_TB_LIST(0, 0, 0, blob);
    final BLAST_TB_LIST test4 = new BLAST_TB_LIST(0, 0, 1, blob);

    ArrayList<BLAST_TB_LIST> l = new ArrayList<>();
    l.add(test1);
    l.add(test2);
    l.add(test3);
    l.add(test4);
    Collections.sort(l);
    for (int i=0;i < l.size() - 1;i++) {

        assertTrue(l.get(i).evalue > l.get(i + 1).evalue ||

                   (l.get(i).evalue == l.get(i + 1).evalue &&
                    l.get(i).score > l.get(i + 1).score) ||

                   (l.get(i).evalue == l.get(i + 1).evalue &&
                    l.get(i).score == l.get(i + 1).score &&
                    l.get(i).seqid <= l.get(i + 1).seqid));
    }
  }



  @Test
  public void testSort2() {
    final byte[] blob = new byte[] {};

    ArrayList<BLAST_TB_LIST> l = new ArrayList<>();
    Random rng = new Random();
    BLAST_TB_LIST test;
    for (int i = 0; i != 10000; ++i) {
      final int evalue = rng.nextInt(50000);
      final int score = rng.nextInt(50000);
      final int seqid = rng.nextInt(50000);
      test = new BLAST_TB_LIST(evalue, score, seqid, blob);
      l.add(test);
    }

    Collections.sort(l);
    for (int i=0;i < l.size() - 1;i++) {
        final BLAST_TB_LIST x = l.get(i);
        final BLAST_TB_LIST next = l.get(i + 1);
        final String why = String.format("%d %d %d <=> %d %d %d", x.evalue,
                                         x.score, x.seqid, next.evalue,
                                         next.score, next.seqid);
        
        assertTrue(why, l.get(i).evalue > l.get(i + 1).evalue ||

                   (l.get(i).evalue == l.get(i + 1).evalue &&
                    l.get(i).score > l.get(i + 1).score) ||

                   (l.get(i).evalue == l.get(i + 1).evalue &&
                    l.get(i).score == l.get(i + 1).score &&
                    l.get(i).seqid <= l.get(i + 1).seqid));
    }
  }

}

