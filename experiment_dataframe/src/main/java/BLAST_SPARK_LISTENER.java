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

import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;

class BLAST_SPARK_LISTENER implements SparkListenerInterface
{
    @Override public final void onStageCompleted( SparkListenerStageCompleted stageCompleted )
    {
        //System.out.println( "listener.stageCompleted" );
    }

    @Override public final void onStageSubmitted( SparkListenerStageSubmitted stageSubmitted )
    {
        //System.out.println( "listener.stageSubmitted" );
    }

    @Override public final void onTaskStart( SparkListenerTaskStart taskStart )
    {
        /*
        TaskInfo ti = taskStart.taskInfo();
        String S = String.format( "taskStart( id:'%s', executor:'%s', status:'%s'",
            ti.id(), ti.executorId(), ti.status() );
        System.out.println( S );
        */
    }

    @Override public final void onTaskGettingResult( SparkListenerTaskGettingResult taskGettingResult )
    {
        //System.out.println( "listener.taskGettingResult" );
    }

    @Override public final void onTaskEnd( SparkListenerTaskEnd taskEnd )
    {
        //System.out.println( "listener.taskEnd" );
    }

    @Override public final void onJobStart( SparkListenerJobStart jobStart )
    {
        //System.out.println( "listener.jobStart" );
    }

    @Override public final void onJobEnd( SparkListenerJobEnd jobEnd )
    {
        //System.out.println( "listener.jobEnd" );
    }

    @Override public final void onEnvironmentUpdate( SparkListenerEnvironmentUpdate environmentUpdate )
    {
        //System.out.println( "listener.environmentUpdate" );
    }

    @Override public final void onBlockManagerAdded( SparkListenerBlockManagerAdded blockManagerAdded )
    {
        //System.out.println( "listener.blockManagerAdded" );
    }

    @Override public final void onBlockManagerRemoved( SparkListenerBlockManagerRemoved blockManagerRemoved )
    {
        //System.out.println( "listener.blockManagerRemoved" );
    }

    @Override public final void onUnpersistRDD( SparkListenerUnpersistRDD unpersistRDD )
    {
        //System.out.println( "listener.unpersistRDD" );
    }

    @Override public final void onApplicationStart( SparkListenerApplicationStart applicationStart )
    {
        System.out.println( "listener.applicationStart" );
    }

    @Override public final void onApplicationEnd( SparkListenerApplicationEnd applicationEnd )
    {
        System.out.println( "listener.applicationEnd" );
    }

    @Override public final void onExecutorMetricsUpdate( SparkListenerExecutorMetricsUpdate executorMetricsUpdate )
    {
        //System.out.println( "listener.executorMetricsUpdate" );
    }

    @Override public final void onExecutorAdded( SparkListenerExecutorAdded event )
    {
        ExecutorInfo ei = event.executorInfo();
        String S = String.format( "exec added: %s", ei.executorHost() );
        System.out.println( S );
    }

    @Override public final void onExecutorRemoved( SparkListenerExecutorRemoved event )
    {
        /*
        String S = String.format( "exec removed: '%s' because '%s'", event.executorId(), event.reason() );
        System.out.println( S );
        */
    }

/*
    @Override public final void onExecutorBlacklisted( SparkListenerExecutorBlacklisted executorBlacklisted )
    {
        //System.out.println( "listener.executorBlacklisted" );
    }

    @Override public void onExecutorBlacklistedForStage( SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage )
    {
        System.out.println( "listener.executorBlacklistedForStage" );
    }

    @Override public void onNodeBlacklistedForStage( SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage )
    {
        System.out.println( "listener.nodeBlacklistedForStage" );
    }

    @Override public final void onExecutorUnblacklisted( SparkListenerExecutorUnblacklisted executorUnblacklisted )
    {
        System.out.println( "listener.executorUnblacklisted" );
    }

    @Override public final void onNodeBlacklisted( SparkListenerNodeBlacklisted nodeBlacklisted )
    {
        System.out.println( "listener.nodeBlacklisted" );
    }

    @Override public final void onNodeUnblacklisted( SparkListenerNodeUnblacklisted nodeUnblacklisted )
    {
        System.out.println( "listener.nodeUnblacklisted" );
    }
*/
    @Override public void onBlockUpdated( SparkListenerBlockUpdated blockUpdated )
    {
        //System.out.println( "listener.blockUpdated" );
    }

/*
    @Override public void onSpeculativeTaskSubmitted( SparkListenerSpeculativeTaskSubmitted speculativeTask )
    {
        System.out.println( "listener.speculativeTask" );
    }
*/
    @Override public void onOtherEvent( SparkListenerEvent event )
    {
        //System.out.println( "listener.other_event" );
    }
}

