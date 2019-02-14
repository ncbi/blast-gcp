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

package gov.nih.nlm.ncbi.blast_spark_cluster;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

public class BC_DATABASE_SETTING implements Serializable
{
    public String key = "";       		/* key for mapping to request... ( 'nt', 'nr' ) */
    public String worker_location = "/tmp/blast/db"; /* where is the location root on the worker? dflt: '/tmp/blast/db' */
	public String source_location = "";	/* bucket or filesystem-path */
    public Boolean flat_layout = false; /* do we use a subdir for each volume under the location on the worker? */
	public List< String > extensions;	/* for nt: nsq, nin, nhr / nr: psq, pin, phr */

    public BC_DATABASE_SETTING()
    {
		extensions = new ArrayList();
    }

    public Boolean valid()
    {
        return ( !key.isEmpty() && !worker_location.isEmpty() && !source_location.isEmpty() && !extensions.isEmpty() );
    }

    @Override public String toString()
    {
        String S = String.format( "\t(%s).worker-loc ...... '%s'\n", key, worker_location );
        S =  S  +  String.format( "\t(%s).source-loc ...... '%s'\n", key, source_location );
        S =  S  +  String.format( "\t(%s).flat layout ..... %s\n", key, Boolean.toString( flat_layout ) );
        S =  S  +  String.format( "\t(%s).extensions ...... %s\n", key, extensions );
        return S;
    }
}

