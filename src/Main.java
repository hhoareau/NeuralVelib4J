import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import spark.Spark;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Main {

    static SparkSession spark=null;

    public static MultilayerPerceptronClassificationModel train(MultilayerPerceptronClassifier trainer,Dataset<Row> stations){
        return trainer.fit(stations);
    }


    public static double evaluate(MultilayerPerceptronClassificationModel model, Dataset<Row> test){
        Dataset<Row> result = model.transform(test);
        Dataset<Row> predictionAndLabels = result.select("prediction", "nPlace");
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy").setLabelCol("nPlace");
        return evaluator.evaluate(predictionAndLabels);
    }


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


    //https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&extension=csv&date=20170221&reseau=09
    public static Map<String,Double> getMeteo(Date dt) throws IOException {
        Map<String,Double> rc=new HashMap<>();

        String sDate=new SimpleDateFormat("yyyyMMdd").format(dt);
        String path="synop."+sDate+"09.csv";
        String url="https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop."+sDate+"09.csv";
        File f=new File(path);
        if(!f.exists()){
            Response r=ClientBuilder.newClient().target(url).request(MediaType.TEXT_PLAIN).get();
            FileOutputStream fo=new FileOutputStream(f);
            fo.write(r.readEntity(String.class).getBytes());
            fo.close();
        }
        for(String[] s:getCSV(path))
            if(s[0].equals("07190")){
                rc.put("temperature",Double.valueOf(s[7])-270);
                rc.put("uv",Double.valueOf(s[9]));
                break;
            }
        return rc;
    }


    //https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json
    public static JsonNode getData(String str) throws IOException {
        try {
            if(str.startsWith("http")){
                URL url = new URL(str);
                URLConnection connection = url.openConnection();
                InputStream is = connection.getInputStream();
                return new ObjectMapper().readTree(is);
            } else {
                FileInputStream f=new FileInputStream(str);
                return new ObjectMapper().readTree(new InputStreamReader(f));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Dataset<Row> createTrain(String file){
        return spark.read().json(file);
    }


    public static Dataset<Row> createTrain(JsonNode jsonNode) throws IOException {
        List<Station> stations= new ArrayList<>();

        Iterator<JsonNode> ite=jsonNode.getElements();
        Double temperature=0.0;
        int uv=0;
        Map<String,Double> data=getMeteo(new Date(System.currentTimeMillis()));

        while(ite.hasNext())
            stations.add(new Station(ite.next().get("fields"), data.get("temperature"), System.currentTimeMillis()));

        //stations=stations.subList(0,10);

        Dataset<Row> rc=spark.createDataFrame(stations,Station.class);
        rc.persist();

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new Station().getCols())
                .setOutputCol("features");

        return assembler.transform(rc);
    }


    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    public static MultilayerPerceptronClassifier trainer=null;

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


    public static MultilayerPerceptronClassificationModel model=null;

    public static String showWeights(MultilayerPerceptronClassificationModel model){
        String s="";
        for(Double d:model.weights().toArray())s=s+"w="+d+"<br>";
        return s;
    }

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException {
        Spark.port(8080);

        createCertificate();

        // create the trainer and set its parameters
        trainer = new MultilayerPerceptronClassifier()
                .setLayers(new int[] {new Station().getCols().length,8,7,3})
                .setBlockSize(128)
                .setSeed(1234L)
                .setLabelCol("nPlace")
                .setMaxIter(1000);

        spark= SparkSession.builder().master("local").appName("Java Spark SQL basic example").getOrCreate();

        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    //String url="https://api.jcdecaux.com/vls/v1/stations/{station_number}?contract=030b9a75c4671dad26255107f2c93dbf710f7bdc";
                    model=train(trainer,createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json")));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,2, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);

        //test : http://localhost:8080/use/1022/21/16/0
        Spark.get("/use/:station/:day/:hour/:soleil", (request, response) -> {
            Station station=new Station(request.params("station"),request.params("day"),request.params("hour"),Double.valueOf(request.params("soleil")));
            return model.predict(station.toVector());
        });


        Spark.get("/evaluate", (request, response) -> {
            return evaluate(model,createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json")));
        });

        Spark.get("/train", (request, response) -> {
            Dataset<Row> datas = createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json"));
            model=train(trainer,datas);
            return evaluate(model,datas);
        });

        Spark.get("/weights", (request, response) -> {
            return showWeights(model);
        });
    }

}
