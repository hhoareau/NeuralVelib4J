package org.neural;

import org.apache.spark.mllib.linalg.Matrix;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Tools {

    private static Logger logger = Logger.getLogger(String.valueOf(Tools.class));

    /**
     *
     * @param str
     * @return
     * @throws IOException
     */
    public static List<String[]> getCSV(String str) throws IOException {
        List<String[]> rc=new ArrayList<>();

        BufferedReader br = new BufferedReader(new FileReader(str));
        String line = null;

        String[] col=null;

        int i=0;
        while ((line = br.readLine()) != null) {
            if(i==0)col=line.split(";");
            String[] values = line.split(";");
            rc.add(values);
            i++;
        }
        br.close();
        return rc;
    }


    public static List<Station> getStations(JsonNode jsonNode,Double temperature) throws ParseException {
        List<Station> rc=new ArrayList<>();
        Iterator<JsonNode> ite=jsonNode.getElements();
        if(ite!=null)
            while(ite.hasNext()){
                JsonNode item=ite.next().get("fields");
                if(item!=null && item.has("status") && item.get("status").asText().equals("OPEN")){
                    Station s=new Station(item, temperature);
                    rc.add(s);
                }
            }
        return rc;
    }


    public static String toHTML(Matrix m){
        String rc="<table style='backgroundColor:grey'>";
        scala.collection.Iterator<org.apache.spark.mllib.linalg.Vector> ite=m.rowIter();
        while(ite.hasNext()){
            org.apache.spark.mllib.linalg.Vector v=ite.next();
            rc+="<tr>";
            for(int j=0;j<m.numCols();j++)
                rc+="<td>"+v.toArray()[j]+"</td>";
            rc+="</tr>";
        }
        rc+="</table>";
        return rc;
    }



    //https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json
    public static JsonNode getData(String str, String copy) throws IOException {
        try {
            if(str.startsWith("http")){
                URL url = new URL(str);
                URLConnection connection = url.openConnection();
                InputStream is = connection.getInputStream();
                String s="";
                if(copy!=null){
                    FileOutputStream f=new FileOutputStream(copy);
                    int read = 0;
                    byte[] bytes = new byte[1024];
                    while ((read = is.read(bytes)) != -1) {
                        f.write(bytes, 0, read);
                    }
                    f.close();
                    return new ObjectMapper().readTree(new FileInputStream(new File(copy)));
                }
                return new ObjectMapper().readTree(is);
            } else {
                FileInputStream f=new FileInputStream(str);
                return new ObjectMapper().readTree(new InputStreamReader(f));
            }
        } catch (IOException e) {
            logger.severe("Probleme de format avec "+str);
        }
        return null;
    }

    public static JsonNode getDataFromFile(String filename, String copy) throws IOException {
        try {
            return getData(filename,copy);
        } catch (JsonParseException e) {
            logger.severe("Probleme de format avec "+filename);
            e.printStackTrace();
            return null;
        }
    }

    public static JsonNode getData(String copy) throws IOException {
        return getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",copy);
    }

    public static void createCertificate() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }




    //https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&extension=csv&date=20170221&reseau=09
    public static Map<String,Double> getMeteo(Date dt) throws IOException {
        Map<String,Double> rc=new HashMap<>();

        String sDate=new SimpleDateFormat("yyyyMMdd").format(dt);
        String path="./files/synop."+sDate+"09.csv";

        File f=new File(path);
        String content="";
        if(!f.exists()){
            for(int server=0;server<10;server++){
                String url="https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop."+sDate+"0"+server+".csv";
                Response r= ClientBuilder.newClient().target(url).request(MediaType.TEXT_PLAIN).get();
                if(r.getStatus()==200)content=r.readEntity(String.class);
                if(content.startsWith("numer_sta"))break;
            }
            if(content.startsWith("numer_sta")){
                FileOutputStream fo=new FileOutputStream(f);
                fo.write(content.getBytes());
                fo.close();
            }
        }
        for(String[] s:getCSV(path))
            if(s[0].equals("07190")){
                rc.put("temperature",Double.valueOf(s[7])-270);
                rc.put("uv",Double.valueOf(s[9]));
                break;
            }
        return rc;
    }

}
