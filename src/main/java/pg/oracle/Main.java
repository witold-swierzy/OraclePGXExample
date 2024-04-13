package pg.oracle;

import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;

import java.net.URI;
import java.net.http.*;
import org.json.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    static String PGX_URL=System.getenv("PGX_URL").replace("\"","");
    static String PGX_USERNAME=System.getenv("PGX_USERNAME").replace("\"","");
    static String PGX_PASSWORD=System.getenv("PGX_PASSWORD").replace("\"","");
    static String PGX_GRAPH=System.getenv("PGX_GRAPH").replace("\"","");
    static String PGX_QUERY=System.getenv("PGX_QUERY").replace("\"","");
    static String token;

    public static void RESTlogin() {
        try {
            String AuthBody = "{\"username\":"+
                    "\""+PGX_USERNAME+"\""+
                    ",\"password\":"+
                    "\""+PGX_PASSWORD+"\""+
                    ",\"creaateSession\":true}";
            HttpClient pgxServer = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(PGX_URL + "/auth/token"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(AuthBody))
                    .build();
            HttpResponse<String> response = pgxServer.send(request, HttpResponse.BodyHandlers.ofString());
            JSONObject obj = new JSONObject(response.body());
            token = obj.getString("access_token");
            System.out.println(token);
        }
        catch (Exception e) {e.printStackTrace();}
    }

    public static void RESTQuery() {
        RESTlogin();
    }
    public static void APIQuery() {
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
        APIQuery();
    }
}