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

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import java.nio.ByteBuffer;

/**
 * utility-class to collect BLAST_TB_LIST classes and join them into a ByteBuffer
 * @see BLAST_TB_LIST
*/
public class BC_RESULTS
{
    private List< BLAST_TB_LIST > l;

/**
 * create instance of BC_RESULTS
 * - create empty list of BLAST_TB_LIST references
 *
 * @see        BLAST_TB_LIST
*/
    public BC_RESULTS()
    {
        l = new ArrayList<>();
    }

/**
 * add all instance in the given list to the internal list
 *
 * @param      items       list of BLAST_TB_LIST-instance to be added
 * @see        BLAST_TB_LIST
*/
    public void add( List< BLAST_TB_LIST > items )
    {
        l.addAll( items );
    }

/**
 * sort the internal list, the item-class has an overriden comparison-method
 *
 * @see        BLAST_TB_LIST
*/
    public void sort()
    {
        Collections.sort( l );
    }

/**
 * cut the internal list, to leave just the top num_items
 *
 * @see        BLAST_TB_LIST
*/
    public void cutoff( int num_items )
    {
        List< BLAST_TB_LIST > sub = l.subList( 0, num_items );
        l = sub;
    }

/**
 * convert internal list of BLAST_TB_LIST-instances into a ByteBuffer
 *
 * @return      ByteBuffer containing valid asn1 to be written to file
 * @see         BLAST_TB_LIST
*/
    public ByteBuffer to_bytebuffer()
    {
        int sum = 0;

        for ( BLAST_TB_LIST e : l )
            sum += e.asn1_blob.length;

        byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
        sum += seq_annot_prefix.length;

        byte[] seq_annot_suffix = { 0, 0, 0, 0, 0, 0, 0, 0 };
        sum += seq_annot_suffix.length;

        ByteBuffer buf = ByteBuffer.allocate( sum );

        buf.put( seq_annot_prefix );

        for ( BLAST_TB_LIST e : l )
            buf.put( e.asn1_blob );

        buf.put( seq_annot_suffix );

        return buf;
    }
}

