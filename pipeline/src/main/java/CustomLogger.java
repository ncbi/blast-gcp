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
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.logging.v2.Logging;
import com.google.api.services.logging.v2.LoggingScopes;
import com.google.api.services.logging.v2.model.LogEntry;
import com.google.api.services.logging.v2.model.MonitoredResource;
import com.google.api.services.logging.v2.model.WriteLogEntriesRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.logging.type.LogSeverity;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CustomLogger {

  private String _log_name;
  private String _app_name;
  private String _job_id;
  private MonitoredResource _monitoredResource;

  private Logging _logging;

  //
  public CustomLogger (String log_name, String app_name, String resource_type, String job_id) {
    try {
        _log_name = log_name;
        _app_name = app_name;
        _job_id = job_id;

        _monitoredResource = new MonitoredResource().setType(resource_type);

        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

        if (credential.createScopedRequired()) {
          Collection<String> scopes = LoggingScopes.all();
          credential = credential.createScoped(scopes);
        }

        _logging = new Logging.Builder(transport, jsonFactory, credential).setApplicationName(_app_name).build();
    }
    catch (GeneralSecurityException | IOException e) {
      e.printStackTrace();
    }
  }

  //
  public void debug (String msg) {
    logMsg(LogSeverity.DEBUG.toString(), msg);
  }

  //
  public void info (String msg) {
    logMsg(LogSeverity.INFO.toString(), msg);
  }

  //
  public void warning (String msg) {
    logMsg(LogSeverity.WARNING.toString(), msg);
  }

  //
  public void error (String msg) {
    logMsg(LogSeverity.ERROR.toString(), msg);
  }

  //
  public void critical (String msg) {
    logMsg(LogSeverity.CRITICAL.toString(), msg);
  }

  // 
  public void logMsg (String severity, String msg) {
	Map<String, Object> payload = prepareJson(msg);
    logEntry(severity, payload);
  }

  // intended msg format:
  // "name1=value1;name2=value2;name3=value3..."
  // if msg is free text, placeholders will be used for names, and string tokens for values
  public Map<String, Object> prepareJson (String msg) {
    int ii = 0;
    Map<String, Object> payload = new TreeMap();
    payload.put("message", msg);
	if (_job_id != null) {
		payload.put("job", _job_id);
	}
    StringTokenizer st1 = new StringTokenizer(msg, ";");
	while (st1.hasMoreTokens()) {
		String name = null;
		String value = null;
		String str = st1.nextToken();
		StringTokenizer st2 = new StringTokenizer(str, "=");
		if (st2.countTokens() == 1) {
			name = "msg" + String.valueOf(ii++);
			if (st2.hasMoreTokens()) {
            	value = st2.nextToken();
			}
		} else {
			if (st2.hasMoreTokens()) {
				name = st2.nextToken();
			}
			if (st2.hasMoreTokens()) {
				value = st2.nextToken();
			}
		}

		if (name != null && value != null) {
			payload.put(name, value);
		}
    }
	return payload;
  }

  // 
  public void logEntry(String severity, Map<String, Object> payload) {
    try {
      LogEntry entry =
          new LogEntry()
              .setJsonPayload(payload)
              .setLogName(_log_name)
              .setSeverity(severity)
              .setResource(_monitoredResource);
      WriteLogEntriesRequest writeLogEntriesRequest =
          new WriteLogEntriesRequest().setEntries(Lists.newArrayList(entry));
      _logging.entries().write(writeLogEntriesRequest).execute();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}

