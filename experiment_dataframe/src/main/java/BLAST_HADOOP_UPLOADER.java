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

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BLAST_HADOOP_UPLOADER
{
    private static BLAST_HADOOP_UPLOADER instance = null;

    private Configuration conf;
    private FileSystem fs;

    public static BLAST_HADOOP_UPLOADER getInstance()
    {
        if ( instance == null )
            instance = new BLAST_HADOOP_UPLOADER();
        return instance;
    }

    private BLAST_HADOOP_UPLOADER()
    {
        try
        {
            conf = new Configuration();
            fs = FileSystem.get( conf );
        }
        catch ( Exception e )
        {
            fs = null;
        }
    }

    private Integer uploadFile( String path, ByteBuffer content )
    {
        Integer res = 0;
        try
        {
            if ( fs != null )
            {
                OutputStream os = fs.create( new Path( path ) );
                os.write( content.array() );
                os.close();
                res = content.array().length;
            }
        }
        catch ( Exception e )
        {
            res = 0;
        }
        return res;
    }

    public static Integer upload( final String path, final ByteBuffer content )
    {
        Integer res = 0;
        BLAST_HADOOP_UPLOADER inst = getInstance();
        if ( inst != null )
            res = inst.uploadFile( path, content );
        return res;
    }

}
