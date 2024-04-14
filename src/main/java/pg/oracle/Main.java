package pg.oracle;

import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;

import java.net.URI;
import java.net.http.*;
import org.json.*;

/* This example presents two ways of using Oracle Property Graph Server :
   1. REST API v.2, as documented here: https://docs.oracle.com/en/database/oracle/property-graph/23.3/spgdg/rest-endpoints-graph-server.html#GUID-7BAF822A-BF21-4709-A929-7B9BAD3870A1
   2. Oracle PGX Server Java API
   Requirements:
   I.  Graph used in the example needs to be loaded and published in another session
   II.  The following environment variables need to be set:
   1. PGX_URL
      this is the main URL for the PGX server.
      example: PGX_URL="http://pgxserver.adomain.com:7007"
   2. PGX_DRIVER
      this is the driver we want to use to query the PGX server
      accepted values are: GRAPH_SERVER_PGX, PGQL_IN_DATABASE, SQL_IN_DATABASE (23c only)
   2. PGX_USERNAME
      this is the PGX username used to log into the PGX server
   3. PGX_PASSWORD
      this is the password for the user of PGX server
   4. PGX_GRAPH
      this is the name of the graph used in the example
      example: PGX_GRAPH=SCHOOL_GRAPH
      NOTE:
      Graph used in the example needs to be loaded and published in another session
   5. PGX_QUERY
      this is the text of the query used in the example
      example: PGX_QUERY="SELECT A.LAST_NAME, A.FIRST_NAME, B.NAME FROM MATCH(A:STUDENTS) - [] - (B:COURSES) ON SCHOOL_GRAPH"
 */

public class Main {
    static String PGX_URL=System.getenv("PGX_URL").replace("\"","");
    //static String PGX_DRIVER=System.getenv("PGX_DRIVER").replace("\"","");
    //static String PGX_DRIVER="PGQL_IN_DATABASE";
    static String PGX_DRIVER="GRAPH_SERVER_PGX";
    static String PGX_USERNAME=System.getenv("PGX_USERNAME").replace("\"","");
    static String PGX_PASSWORD=System.getenv("PGX_PASSWORD").replace("\"","");
    static String PGX_GRAPH=System.getenv("PGX_GRAPH").replace("\"","");
    static String PGX_QUERY=System.getenv("PGX_QUERY").replace("\"","");
    static String token;

    public static void RESTlogin() {
        try {
            String body = "{\"username\":"+
                    "\""+PGX_USERNAME+"\""+
                    ",\"password\":"+
                    "\""+PGX_PASSWORD+"\""+
                    ",\"creaateSession\":true}";
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
            HttpResponse<String> response = pgxServer.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Query execution, response body:\n"+response.body()+"\n");
        }
        catch (Exception e) {e.printStackTrace();}
    }
    public static void RESTQuery() {
        RESTlogin();
        RESTgetGraphs();
        RESTexecuteQuery();
    }
    public static void APIQuery() {
        // still under development
        try {
            PgxGraph graph;
            ServerInstance si = GraphServer.getInstance(PGX_URL,PGX_USERNAME,PGX_PASSWORD.toCharArray());
            PgxSession ses = si.createSession("my-session");
            graph = ses.getGraph(PGX_GRAPH);
            System.out.println(graph);
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void main(String[] args) {
        RESTQuery();
        //APIQuery();
    }
}