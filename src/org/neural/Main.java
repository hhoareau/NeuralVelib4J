package org.neural;

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.Normalizer;
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
import java.util.logging.Logger;


public class Main {

    private static List<Station> stations=new ArrayList<>();
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
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
    public static JsonNode getData(String str,String copy) throws IOException {
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
            e.printStackTrace();
        }
        return null;
    }


    public static Dataset<Row> createTrain(String file){
        return spark.read().json(file);
    }


    public static Dataset<Row> createTrain(JsonNode jsonNode) throws IOException {
        List<Station> l_stations= new ArrayList<>();

        Iterator<JsonNode> ite=jsonNode.getElements();
        Double temperature=0.0;
        int uv=0;
        Map<String,Double> data=getMeteo(new Date(System.currentTimeMillis()));

        while(ite.hasNext())
            l_stations.add(new Station(ite.next().get("fields"), data.get("temperature"), System.currentTimeMillis()));

        if(stations.size()==0)stations.addAll(l_stations);
        Dataset<Row> rc=spark.createDataFrame(l_stations, Station.class);
        rc.persist();

        //rc.show(20,false);
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new Station().colsName())
                .setOutputCol("tempFeatures");
        rc=assembler.transform(rc);
        //rc.show(20,false);

        Normalizer normalizer = new Normalizer()
                .setInputCol("tempFeatures")
                .setOutputCol("features")
                .setP(1.0);
        rc=normalizer.transform(rc);
        //rc.show(20,false);

        return rc;
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
        logger.info("Ouverture du port 9999");
        Spark.port(9999);

        logger.info("Creation d'un certificat pour la navigation https");
        createCertificate();

        // create the trainer and set its parameters
        trainer = new MultilayerPerceptronClassifier()
                .setLayers(new int[] {new Station().colsName().length,200,150,3})
                .setBlockSize(128)
                .setSeed(1234L)
                .setLabelCol("nPlace")
                .setMaxIter(1000);

        spark= SparkSession.builder().master("local").appName("Java Spark SQL basic example").getOrCreate();

        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    //String url="https://api.jcdecaux.com/vls/v1/stations/{station_number}?contract=030b9a75c4671dad26255107f2c93dbf710f7bdc";
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    if(horaire.endsWith("0") || horaire.endsWith("5")){
                        String copyPath="velib_"+System.currentTimeMillis()+".json";
                        model=train(trainer,createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",copyPath)));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);

        //test : http://localhost:9999/use/PARADIS/21/16/0
        Spark.get("/use/:station/:day/:hour/:soleil", (request, response) -> {
            for(Station s:stations)
                if(s.getName().indexOf(request.params("station"))>0){
                    Station station=new Station(s.getId(),request.params("day"),request.params("hour"),Double.valueOf(request.params("soleil")));
                    org.apache.spark.ml.linalg.Vector v=station.toVector().toDense();
                    if(model.numFeatures()==v.size())
                        return model.predict(v);
                    else
                        return "ecart de dimension "+model.numFeatures()+" et input "+v.size();
                }
            return "Aucune station";
        });


        Spark.get("/evaluate", (request, response) -> {
            return evaluate(model,createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",null)));
        });

        Spark.get("/train", (request, response) -> {
            File dir=new File(".");
            Dataset<Row> dataset=null;
            for(File f:dir.listFiles()){
                if(f.getName().indexOf(".json")>0){
                    Dataset<Row> dt = createTrain(getData(f.getName(), null));
                    if(dataset==null)
                        dataset=dt;
                    else
                        dataset=dataset.union(dt);
                }
            }
            model=train(trainer,dataset);
            return evaluate(model,dataset);
        });

        Spark.get("/weights", (request, response) -> {
            return showWeights(model);
        });
    }



}
