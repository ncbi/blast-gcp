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

public class CONF_VOLUME_FILE implements Serializable
{
    public String f_type;
    public String f_name;
    public String f_md5;

    public Boolean valid()
    {
        if ( f_type.isEmpty() ) return false;
        if ( f_name.isEmpty() ) return false;
        if ( f_md5.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( f_type.isEmpty() ) S = S + "manifest.json : volume.files.type missing\n";
        if ( f_name.isEmpty() ) S = S + "manifest.json : volume.files.name missing\n";
        if ( f_md5.isEmpty() ) S = S + "manifest.json : volume.files.md5 missing\n";
        return S;
    }

    @Override public String toString()
    {
        return String.format( "\t\t\t[ type:'%s', name:'%s', md5:'%s'\n", f_type, f_name, f_md5 );
    }
}

