package QueryManagement.Requests;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HttpHandler {
    private final static String USER_AGENT = "Mozilla/5.0";

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public static HttpResponse sendGetRequest(String urlString, int wait, int debug){
        try {

            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(10000);
            connection.setRequestProperty("User-Agent", USER_AGENT);

            BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String content = IOUtils.toString(response);
            int responseCode = connection.getResponseCode();

            if(responseCode == HttpURLConnection.HTTP_OK) {
                HttpResponse httpResponse = new HttpResponse(connection.getHeaderField("Content-Type"),content);
                try {
                    TimeUnit.MILLISECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return httpResponse;
            } else if (responseCode == HttpURLConnection.HTTP_MOVED_PERM
                    || responseCode == HttpURLConnection.HTTP_MOVED_TEMP)
            {
                HttpResponse httpResponse = handleRedirect(connection,debug);
                try {
                    TimeUnit.MILLISECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return httpResponse;
            } else {
                return null;
            }
        } catch (IOException e) {
            System.out.println("HTTP GET ERROR: " + e.getMessage());
        }

        try {
            TimeUnit.MILLISECONDS.sleep(wait);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static HttpResponse sendGetRequest(String urlString){
        return sendGetRequest(urlString,0,0);
    }

    private static HttpResponse handleRedirect(HttpURLConnection connection, int debug) {

        String url = connection.getHeaderField("Location");
        try {
            URL obj = new URL(url);
            HttpURLConnection newCon = (HttpURLConnection) obj.openConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sendGetRequest(url);
    }
}
