package pg.oracle;

import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import oracle.pgx.config.*;
import org.json.*;

/* This example presents two ways of using Oracle Property Graph Server :
   1. REST API v.2, as documented here: https://docs.oracle.com/en/database/oracle/property-graph/23.3/spgdg/rest-endpoints-graph-server.html#GUID-7BAF822A-BF21-4709-A929-7B9BAD3870A1
   2. Oracle PGX Server Java API
   Requirements:
   I.  Graph used in the example needs to be loaded and published in another session
   II.  The following environment variables need to be set:
   1. PGX_EXECUTION_MODE
      this is the mode of the application execution. Following values are accepted:
      PGX_REST_QUERY_MDOE - when the app is executed to query the graph by using REST v.2 calls
      PGX_API_QUERY_MODE   - when the app is executed to query the graph by using Oracle PGX API
      PGX_LOAD_MODE  - when the app is executed to load the graph from the database
      PGX_SYNC_MODE  - when the app is executed to synchronize the graph
   2. PGX_URL
      this is the main URL for the PGX server.
      example: PGX_URL="http://pgxserver.adomain.com:7007"
   3. PGX_DRIVER
      this is the driver we want to use to query the PGX server
      accepted values are: GRAPH_SERVER_PGX, PGQL_IN_DATABASE, SQL_IN_DATABASE (23c only)
   4. PGX_USERNAME
      this is the PGX username used to log into the PGX server
   5. PGX_PASSWORD
      this is the password for the user of PGX server
   6. PGX_GRAPH
      this is the name of the graph used in the example
      example: PGX_GRAPH=SCHOOL_GRAPH
      NOTE:
      Graph used in the example needs to be loaded and published in another session in case, when
   7. PGX_QUERY
      this is the text of the query used in the example
      example: PGX_QUERY="SELECT A.LAST_NAME, A.FIRST_NAME, B.NAME FROM MATCH(A:STUDENTS) - [] - (B:COURSES) ON SCHOOL_GRAPH"
   8. PGX_EXECUTIONS
      this is the number of executions of the query provided inn PGX_QUERY environment variable
   9. PGX_JDBC_URL
      this is the URL for the database connection. Required only in case,
      when the application is executed to load or synchronize graph
  10. PGX_CONFIG_FILE
      this is the name of the JSON config file for the graph.
      this file is created when the graph is loaded into the PGX server memory
      and is read everytime the application is executed to synchronize the graph
 */

public class Main {
    static String PGX_EXECUTION_MODE;
    static String PGX_URL;
    static String PGX_DRIVER;
    static String PGX_USERNAME;
    static String PGX_PASSWORD;
    static String PGX_GRAPH;
    static String PGX_QUERY;
    static int PGX_EXECUTIONS;
    static String PGX_JDBC_URL;
    static String PGX_CONFIG_FILE;
    static String token;


    public static void initApp() {
        //PGX_EXECUTION_MODE = System.getenv("PGX_EXECUTION_MODE").replace("\"","");
        PGX_EXECUTION_MODE = "PGX_SYNC_MODE";
        //PGX_EXECUTION_MODE = "PGX_LOAD_MODE";
        PGX_URL            = System.getenv("PGX_URL").replace("\"","");
        if ( PGX_EXECUTION_MODE.equals("PGX_REST_QUERY_MODE"))
            PGX_DRIVER         = System.getenv("PGX_DRIVER").replace("\"","");
        else
            PGX_DRIVER         = "N/A";
        PGX_USERNAME       = System.getenv("PGX_USERNAME").replace("\"","");
        PGX_PASSWORD       = System.getenv("PGX_PASSWORD").replace("\"","");
        PGX_GRAPH          = System.getenv("PGX_GRAPH").replace("\"","");
        PGX_QUERY          = System.getenv("PGX_QUERY").replace("\"","");
        if ( PGX_EXECUTION_MODE.equals("PGX_REST_QUERY_MODE") || PGX_EXECUTION_MODE.equals("PGX_API_QUERY_MODE") )
            PGX_EXECUTIONS     = Integer.valueOf(System.getenv("PGX_EXECUTIONS").replace("\"",""));
        else
            PGX_EXECUTIONS     = 0;
        if ( PGX_EXECUTION_MODE.equals("PGX_SYNC_MODE"))
            PGX_JDBC_URL       = System.getenv("PGX_JDBC_URL").replace("\"","");
        else
            PGX_JDBC_URL       = "N/A";
        if ( PGX_EXECUTION_MODE.equals("PGX_LOAD_MODE") || PGX_EXECUTION_MODE.equals("PGX_SYNC_MODE") )
            PGX_CONFIG_FILE    = System.getenv("PGX_CONFIG_FILE").replace("\"","");
        else
            PGX_CONFIG_FILE    = "N/A";
    }
    public static void RESTlogin() {
        try {
            String body = "{\"username\":"+
                    "\""+PGX_USERNAME+"\""+
                    ",\"password\":"+
                    "\""+PGX_PASSWORD+"\""+
                    ",\"createSession\":true"+
                    ",\"source\":\"POSTMAN\"}";
            HttpClient pgxServer = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(PGX_URL + "/auth/token"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = pgxServer.send(request, HttpResponse.BodyHandlers.ofString());
            JSONObject obj = new JSONObject(response.body());
            token = obj.getString("access_token");
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void RESTgetGraphs() {
        try {
            HttpClient pgxServer = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(PGX_URL + "/v2/graphs?driver="+PGX_DRIVER))
                    .header("Authorization", "Bearer " + token)
                    .GET()
                    .build();
            HttpResponse<String> response = pgxServer.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Listing graphs, response body:\n"+response.body()+"\n");
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void RESTexecuteQuery() {
        try {
            String body = "{\n"+
                          "   \"statements\": [\n"+
                          "      \""+PGX_QUERY+"\"\n"+
                          "   ],\n"+
                          "   \"driver\": \""+PGX_DRIVER+"\",\n"+
                          "   \"formatter\": \"GVT\",\n"+
                          "   \"visualize\": true\n"+
                          "}";
            HttpClient pgxServer = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(PGX_URL + "/v2/runQuery"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            for ( int i=1; i<=PGX_EXECUTIONS; i++) {
                HttpResponse<String> response = pgxServer.send(request, HttpResponse.BodyHandlers.ofString());
                System.out.println("Query REST execution #"+i+", response body:\n" + response.body() + "\n");
            }
        }
        catch (Exception e) {e.printStackTrace();}
    }
    public static void RESTQuery() {
        RESTlogin();
        RESTgetGraphs();
        RESTexecuteQuery();
    }
    public static void APIQuery() {
        try {
            ServerInstance si = GraphServer.getInstance(PGX_URL,PGX_USERNAME,PGX_PASSWORD.toCharArray());
            PgxSession ses = si.createSession("my-session");
            PgqlResultSet res;
            for (int i=1;i<=PGX_EXECUTIONS;i++) {
                res = ses.queryPgql(PGX_QUERY);
                System.out.println("Query PGX API execution #"+i+":\n"+res.getNumResults());
                res.close();
            }
            ses.close();
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void loadGraph() {
        try {
            ServerInstance si = GraphServer.getInstance(PGX_URL, PGX_USERNAME, PGX_PASSWORD.toCharArray());
            PgxSession ses = si.createSession("my-session");
            Map<String,PgxGraph> m = ses.getGraphs();
            if ( m.containsKey(PGX_GRAPH) )
                System.out.println("Graph "+PGX_GRAPH+" is already loaded and published");
            else {
                PgxGraph graph = ses.readGraphByName(PGX_GRAPH, GraphSource.PG_VIEW);
                GraphConfig config = graph.getConfig();
                PrintWriter writer = new PrintWriter(PGX_CONFIG_FILE);
                writer.print(config);
                writer.close();
                graph.destroy();
                graph = ses.readGraphWithProperties(config, true);
                graph.publishWithSnapshots();
                //graph.pin();
            }
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void synchronizeGraph() {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            Connection conn = DriverManager.getConnection(PGX_JDBC_URL, PGX_USERNAME, PGX_PASSWORD);
            conn.setAutoCommit(false);
            ServerInstance si = GraphServer.getInstance(PGX_URL, PGX_USERNAME, PGX_PASSWORD.toCharArray());
            PgxSession ses = si.createSession("my-session");
            PgxGraph graph = ses.getGraph(PGX_GRAPH);
            PartitionedGraphConfig config = GraphConfigFactory.forPartitioned().fromFilePath(PGX_CONFIG_FILE);
            System.out.println(config);
            Synchronizer synchronizer = new Synchronizer.Builder<FlashbackSynchronizer>()
                    .setType(FlashbackSynchronizer.class)
                    .setGraph(graph)
                    .setConnection(conn)
                    .setGraphConfiguration(config)
                    .build();
            graph = synchronizer.sync();
        }
        catch (Exception e) {e.printStackTrace();}
    }
    public static void main(String[] args) {
        initApp();
        System.out.println("Execution mode       : "+PGX_EXECUTION_MODE);
        System.out.println("Query mode           : "+PGX_DRIVER);
        System.out.println("Number of executions : "+PGX_EXECUTIONS);
        System.out.println(PGX_JDBC_URL);
        long start = System.currentTimeMillis();
        if ( PGX_EXECUTION_MODE.equals("PGX_REST_QUERY_MODE"))
            RESTQuery();
        if ( PGX_EXECUTION_MODE.equals("PGX_API_QUERY_MODE"))
            APIQuery();
        if ( PGX_EXECUTION_MODE.equals("PGX_LOAD_MODE"))
            loadGraph();
        if ( PGX_EXECUTION_MODE.equals("PGX_SYNC_MODE"))
            synchronizeGraph();
        long end = System.currentTimeMillis();
        System.out.println("Execution mode       : "+PGX_EXECUTION_MODE);
        System.out.println("Query mode           : "+PGX_DRIVER);
        System.out.println("Number of executions : "+PGX_EXECUTIONS);
        System.out.println("Execution completed successfully");
        System.out.println("Execution total elapsed time in milliseconds: "+(end-start));
    }
}