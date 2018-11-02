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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

public class MD5ForFile
{

    private File file;

    public MD5ForFile( final String filePath )
	{
        this.file = new File( filePath );
    }

    public MD5ForFile( final File file )
	{
        this.file = file;
    }

    public String getMD5()
	{
        String md5 = null;

        FileInputStream fileInputStream = null;

        try
		{
            fileInputStream = new FileInputStream( this.file );

            // md5Hex converts an array of bytes into an array of characters representing the hexadecimal values of each byte in order.
            // The returned array will be double the length of the passed array, as it takes two characters to represent any given byte.

            md5 = DigestUtils.md5Hex( IOUtils.toByteArray( fileInputStream ) );

            fileInputStream.close();

        }
		catch ( IOException e )
		{
            e.printStackTrace();
        }

        return md5;
    }
}

