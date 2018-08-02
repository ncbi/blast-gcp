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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Utility class for this sample application.
 */
public final class PubSubUtils {

  /**
   * The application name will be attached to the API requests.
   */
  private static final String APP_NAME = "cloud-pubsub-sample-cli/1.0";

  /**
   * Prevents instantiation.
   */
  private PubSubUtils() {
  }

  /**
   * Enum representing a resource type.
   */
  public enum ResourceType {
    /**
     * Represents topics.
     */
    TOPIC("topics"),
    /**
     * Represents subscriptions.
     */
    SUBSCRIPTION("subscriptions");
    /**
     * A path representation for the resource.
     */
    private String collectionName;
    /**
     * A constructor.
     *
     * @param collectionName String representation of the resource.
     */
    private ResourceType(final String collectionName) {
      this.collectionName = collectionName;
    }
    /**
     * Returns its collection name.
     *
     * @return the collection name.
     */
    public String getCollectionName() {
      return this.collectionName;
    }
  }

  /**
   * Returns the fully qualified resource name for Pub/Sub.
   *
   * @param resourceType ResourceType.
   * @param project A project id.
   * @param resource topic name or subscription name.
   * @return A string in a form of PROJECT_NAME/RESOURCE_NAME
   */
    public static String getFullyQualifiedResourceName( final ResourceType resourceType,
                    final String project, final String resource )
    {
        return String.format("projects/%s/%s/%s", project, resourceType.getCollectionName(), resource );
    }

  /**
   * Builds a new Pubsub client with default HttpTransport and
   * JsonFactory and returns it.
   *
   * @return Pubsub client.
   * @throws IOException when we can not get the default credentials.
   */
    public static Pubsub getClient() throws IOException
    {
        return getClient(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory() );
    }

  /**
   * Builds a new Pubsub client and returns it.
   *
   * @param httpTransport HttpTransport for Pubsub client.
   * @param jsonFactory JsonFactory for Pubsub client.
   * @return Pubsub client.
   * @throws IOException when we can not get the default credentials.
   */
    public static Pubsub getClient( final HttpTransport httpTransport, final JsonFactory jsonFactory ) throws IOException
    {
        Preconditions.checkNotNull( httpTransport );
        Preconditions.checkNotNull( jsonFactory );
        GoogleCredential credential = GoogleCredential.getApplicationDefault( httpTransport, jsonFactory );
        if ( credential.createScopedRequired() )
        {
            credential = credential.createScoped( PubsubScopes.all() );
        }
        // Please use custom HttpRequestInitializer for automatic
        // retry upon failures.
        HttpRequestInitializer initializer = new RetryHttpInitializerWrapper( credential );
        return new Pubsub.Builder( httpTransport, jsonFactory, initializer )
            .setApplicationName(APP_NAME)
            .build();
    }


}

