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

package gov.nih.nlm.ncbi.exp;

import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

public class EXP_RDD_CONFIG< T > implements Serializable
{
    private Class< T > entityClass;
    private int num_partitions;
    private int num_locations;
    private List< String > locations;

    public EXP_RDD_CONFIG( Class< T > a_entityClass, int a_num_partitions, List< String > a_locations )
    {
        entityClass = a_entityClass;
        num_partitions = a_num_partitions;
        locations = new ArrayList<>();
        for ( String l : a_locations )
            locations.add( l );
        num_locations = locations.size();
    }

    public Class< T > getEntityClass()
    {
        return entityClass;
    }

    public int get_num_partitions()
    {
        return num_partitions;
    }

    public List< String > get_locations()
    {
        return locations;
    }

    public String get_location_by_idx( int idx )
    {
        if ( idx < num_locations )
            return locations.get( idx );
        else
            return locations.get( idx % num_locations );
    }
}

