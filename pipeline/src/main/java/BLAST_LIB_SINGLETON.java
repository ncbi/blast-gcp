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

class BLAST_LIB_SINGLETON
{
    // let us have a map of PARTITION-ID to BLAST_LIB
    private static BLAST_LIB_SINGLETON instance = null;
    private static Integer requested = 0;
    private BLAST_LIB blaster;

    /* this ensures that nobody can make an instance of this class, but the class itself */
    private BLAST_LIB_SINGLETON()
    {
        blaster = new BLAST_LIB();
    }

    private static BLAST_LIB_SINGLETON getInstance()
    {
        if ( instance == null )
            instance = new BLAST_LIB_SINGLETON();
        return instance;
    }

    public static BLAST_LIB get_lib()
    {
        BLAST_LIB_SINGLETON inst = getInstance();
        requested += 1;
        return inst.blaster;
    }

    public static Integer get_requests()
    {
        return requested;
    }
}

