package gov.nih.nlm.ncbi.blastjni;

import static org.junit.Assert.*;

import org.junit.*;

public class Test_BLAST_TB_LIST {
  @Test
  public void testGetters() {
    final byte[] blob1 = new byte[] {};
    final byte[] blob2 = new byte[] {0x01};
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 1.0, blob1);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1.0, blob2);

    assertTrue(test1.isEmpty());
    assertFalse(test2.isEmpty());
  }

  @Test
  public void testCompares() {
    final double delta = BLAST_TB_LIST.epsilon / 10.0;
    final byte[] blob = new byte[] {};
    // These should be listed in desired sorting order
    final BLAST_TB_LIST test5 = new BLAST_TB_LIST(1, -2.0, blob);
    final BLAST_TB_LIST test3 = new BLAST_TB_LIST(1, 1.0 - delta, blob);
    final BLAST_TB_LIST test1 = new BLAST_TB_LIST(1, 1.0, blob);
    final BLAST_TB_LIST test6 = new BLAST_TB_LIST(0, 1.0, blob);
    final BLAST_TB_LIST test2 = new BLAST_TB_LIST(1, 1.0 + delta, blob);
    final BLAST_TB_LIST test4 = new BLAST_TB_LIST(1, 2.0, blob);
    final BLAST_TB_LIST test7 = new BLAST_TB_LIST(1, 3.0, blob);

    assertEquals("reflexive", test1.compareTo(test1), 0);
    assertEquals("reflexive", test2.compareTo(test2), 0);
    assertEquals("reflexive", test3.compareTo(test3), 0);
    assertEquals("reflexive", test4.compareTo(test4), 0);
    assertEquals("reflexive", test5.compareTo(test5), 0);
    assertEquals("reflexive", test6.compareTo(test6), 0);

    assertEquals(test1.compareTo(test6), 1);
    assertEquals(test6.compareTo(test1), -1);

    assertEquals("symmetric", test1.compareTo(test2), 0);
    assertEquals("symmetric", test1.compareTo(test3), 0);
    assertEquals("symmetric", test2.compareTo(test3), 0);

    assertEquals(test1.compareTo(test4), -1);
    assertEquals(test4.compareTo(test1), 1);

    assertEquals(test1.compareTo(test5), 1);
    assertEquals(test5.compareTo(test1), -1);

    assertEquals(test4.compareTo(test7), -1);
    assertEquals(test7.compareTo(test4), 1);

    assertEquals("transitive", test5.compareTo(test7), -1);
    assertEquals("transitive", test7.compareTo(test5), 1);

    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test1.compareTo(test2),
        BLAST_TB_LIST.FuzzyEvalueComp(test1.evalue, test2.evalue));
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test1.compareTo(test4),
        BLAST_TB_LIST.FuzzyEvalueComp(test1.evalue, test4.evalue));
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test4.compareTo(test1),
        BLAST_TB_LIST.FuzzyEvalueComp(test4.evalue, test1.evalue));
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test1.compareTo(test4),
        BLAST_TB_LIST.FuzzyEvalueComp(test1.evalue, test4.evalue));
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test5.compareTo(test7),
        BLAST_TB_LIST.FuzzyEvalueComp(test5.evalue, test7.evalue));

    final BLAST_TB_LIST test8 = new BLAST_TB_LIST(0, 6.225763, blob);
    final BLAST_TB_LIST test9 = new BLAST_TB_LIST(0, 0.511042, blob);
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test8.compareTo(test9),
        BLAST_TB_LIST.FuzzyEvalueComp(test8.evalue, test9.evalue));
    assertEquals(
        "compareTo and FuzzyEvalueComp disagree",
        test9.compareTo(test8),
        BLAST_TB_LIST.FuzzyEvalueComp(test9.evalue, test8.evalue));
  }
}
