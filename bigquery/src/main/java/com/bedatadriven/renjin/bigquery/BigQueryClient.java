package com.bedatadriven.renjin.bigquery;


import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.googleapis.notifications.StoredChannel;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStore;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.api.client.util.store.FileDataStoreFactory;

import javax.activation.FileDataSource;
import java.io.*;
import java.util.Arrays;
import java.util.List;

public class BigQueryClient {
  private static final String PROJECT_ID = "bdd-dev";
  private static final String CLIENT_SECRETS_LOCATION = "client_secrets.json";

  private static final List SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);

  private static String REDIRECT_URI = "oob";

  private GoogleAuthorizationCodeFlow flow;
  private GoogleClientSecrets clientSecrets;

  private com.google.api.client.util.store.FileDataStoreFactory datastoreFactory;
  private Bigquery bigquery;

  public GoogleClientSecrets loadClientSecrets() {
    try {
      Reader reader = Resources.asCharSource(
          Resources.getResource(BigQueryClient.class, CLIENT_SECRETS_LOCATION), Charsets.UTF_8)
          .openStream();
      clientSecrets = GoogleClientSecrets.load(new JacksonFactory(), reader);
      return clientSecrets;
    } catch (Exception e) {
      throw new RuntimeException("Could not load client_secrets.json");
    }
  }

  /**
   * Build an authorization flow and store it as a static class attribute.
   *
   * @return a Google Auth flow object
   */
  private GoogleAuthorizationCodeFlow getFlow() throws IOException {
    if (flow == null) {
      HttpTransport httpTransport = new NetHttpTransport();
      JacksonFactory jsonFactory = new JacksonFactory();

      datastoreFactory = new FileDataStoreFactory(new File("/home/alex"));
      DataStore<StoredCredential> cred = datastoreFactory.<StoredCredential>getDataStore("cred");
      flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport,
          jsonFactory,
          loadClientSecrets(),
          SCOPES)
          .setAccessType("offline")
          .setCredentialDataStore(cred)
          .setApprovalPrompt("force").build();
    }

    return flow;
  }

  Credential exchangeCode(String authorizationCode) throws IOException {
    GoogleAuthorizationCodeFlow flow = getFlow();
    GoogleTokenResponse response =
        flow.newTokenRequest(authorizationCode).setRedirectUri(REDIRECT_URI).execute();
    return flow.createAndStoreCredential(response, "user");
  }

  Credential getCredentials() throws IOException {
    Credential credential = getFlow().loadCredential("user");
    if(credential == null) {
      String authorizeUrl =
          new GoogleAuthorizationCodeRequestUrl(loadClientSecrets(),
              REDIRECT_URI,
              SCOPES).setState("").build();

      System.out.println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl);

      System.out.println("... and type the code you received here: ");
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      String authorizationCode = in.readLine();
      credential = exchangeCode(authorizationCode);
    }
    return credential;
  }

  public void test() throws IOException {

    System.out.println(getCredentials());
    // Refresh key
    //1/atzN0jsJT_pOFR2LwlU2EBX1_eM1Avjzn4qDdLqPE3s

  }

  public void getData() throws IOException, InterruptedException {
    HttpTransport TRANSPORT = new NetHttpTransport();
    JsonFactory JSON_FACTORY = new JacksonFactory();


    bigquery = new Bigquery(TRANSPORT, JSON_FACTORY, getCredentials());

    String querySql = "SELECT word, word_count FROM publicdata:samples.shakespeare LIMIT 10";

    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);

    job.setConfiguration(config);
    queryConfig.setQuery(querySql);

    Bigquery.Jobs.Insert insert = bigquery.jobs().insert(PROJECT_ID, job);
    JobReference jobId = insert.execute().getJobReference();

    System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

    Job completedJob = waitFor(jobId);

    GetQueryResultsResponse queryResult = bigquery.jobs()
        .getQueryResults(
            PROJECT_ID, completedJob
            .getJobReference()
            .getJobId()
        ).execute();
    List<TableRow> rows = queryResult.getRows();
    System.out.print("\nQuery Results:\n------------\n");
    for (TableRow row : rows) {
      for (TableCell field : row.getF()) {
        System.out.printf("%-50s", field.getV());
      }
      System.out.println();
    }
  }


  public Job waitFor(JobReference job) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;

    while (true) {
      Job pollJob = bigquery.jobs().get(PROJECT_ID, job.getJobId()).execute();
      elapsedTime = System.currentTimeMillis() - startTime;
      System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
          job.getJobId(), pollJob.getStatus().getState());
      if (pollJob.getStatus().getState().equals("DONE")) {
        return pollJob;
      }
      // Pause execution for one second before polling job status again
      Thread.sleep(1000);
    }
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    new BigQueryClient().getData();
  }
}
