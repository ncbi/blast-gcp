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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.common.collect.Lists;

public final class BLAST_PUBSUB  extends Thread
{
    private final BLAST_STATUS status;
    private final BLAST_SETTINGS settings;
    private final Integer sleep_time;
    private Pubsub client;
    private String subscriptionName;

    public BLAST_PUBSUB( final BLAST_STATUS a_status,
                         final BLAST_SETTINGS a_settings,
                         final Integer a_sleep_time )
    {
        this.status = a_status;
        this.settings = a_settings;
        this.sleep_time = a_sleep_time;
        this.client = getClient();
        this.subscriptionName = PubSubUtils.getFullyQualifiedResourceName(
            PubSubUtils.ResourceType.SUBSCRIPTION, settings.project_id, settings.subscript_id );

    }

    private Pubsub getClient()
    {
        try
        {
            return PubSubUtils.getClient();
        }
        catch( IOException e )
        {
            System.out.println( "BLAST_PUBSUB: getClient : " + e.toString() );
        }
        return null;
    }

    private List< ReceivedMessage > pullMessages( Integer maxMessages )
    {
        try
        {
            PullRequest pullRequest = new PullRequest().setReturnImmediately( true ).setMaxMessages( maxMessages );
            PullResponse pullResponse = client.projects().subscriptions().pull( subscriptionName, pullRequest ).execute();
            return pullResponse.getReceivedMessages();
        }
        catch( IOException e )
        {
            System.out.println( "BLAST_PUBSUB: pullMessages : " + e.toString() );
        }
        return null;
    }

    private void ackMessage( String id )
    {
        try
        {
            AcknowledgeRequest ackRequest = new AcknowledgeRequest();
            ackRequest.setAckIds( Lists.newArrayList( id ) );
            client.projects().subscriptions().acknowledge( subscriptionName, ackRequest ).execute();
        }
        catch( IOException e )
        {
            System.out.println( "BLAST_PUBSUB: ackMessages : " + e.toString() );
        }
    }

    private void do_sleep()
    {
        try
        {
            Thread.sleep( sleep_time );
        }
        catch ( InterruptedException e )
        {
        }
    }

    private String decode( ReceivedMessage msg )
    {
        try
        {
            return new String( msg.getMessage().decodeData(), "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            System.out.println( "BLAST_PUBSUB: decode() : " + e.toString() );            
        }
        return null;
    }

    @Override public void run()
    {
        System.out.println( "Subscribing to " + subscriptionName );

        while( status.is_running() )
        {
            boolean perform_sleep = true;
            String ack;

            int can_take = status.can_take();
            if ( can_take > 0 )
            {
                List< ReceivedMessage > messages = pullMessages( can_take );
                if ( messages != null )
                {
                    for( ReceivedMessage msg : messages )
                    {
                        String msg_string = decode( msg );
                        if ( msg_string != null )
                        {
                            ack = msg.getAckId();
                            REQUESTQ_ENTRY re = BLAST_REQUEST_READER.parse_from_string_and_ack( msg_string, ack, settings.top_n );
                            if ( re == null )
                            {
                                System.out.println( "REQUEST from PUBSUB invalid" );
                                ackMessage( ack );

                                String gs_status_key = String.format( settings.gs_status_file, re.request.id );
                                BLAST_GS_UPLOADER.upload( settings.gs_status_bucket, gs_status_key, settings.gs_status_error );
                            }
                            else
                            {
                                if ( !status.add_request( re, settings.log.request ? System.out : null ) )
                                    status.update_ack( re );
                            }
                        }
                        perform_sleep = false;
                    };
                }
            }

            ack = status.get_ack();
            if ( ack != null )
            {
                ackMessage( ack );
                perform_sleep = false;
            }

            if ( perform_sleep )
                do_sleep();
        }

        System.out.println( "pubsub done " + subscriptionName );
    }
}

