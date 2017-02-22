package org.neural;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import scala.Array;
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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;

public class Main {

    private static List<Station> stations=new ArrayList<>();
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
    static SparkSession spark=null;

    public static PipelineModel train(Pipeline trainer,Dataset<Row> stations) throws IOException {
        PipelineModel model=trainer.fit(stations);
        save((MultilayerPerceptronClassificationModel)model.stages()[0]);
        return model;
    }

    public static void save(MultilayerPerceptronClassificationModel mlp_model) throws IOException {
        org.apache.spark.ml.linalg.Vector v = mlp_model.weights();
        FileOutputStream f=new FileOutputStream("velib.w");
        String s="";
        for(Double d:v.toArray())s+=d+";";
        f.write(s.getBytes());
        f.close();
    }



    public static Pipeline createPipeline()  {
        // create the trainer and set its parameters
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier()
                .setLayers(new int[] {new Station().colsName().length,10,10,3})
                .setBlockSize(128)
                .setSeed(1234L)
                .setLabelCol("nPlace")
                .setMaxIter(10);

        load(mlp);

        Pipeline pip=new Pipeline().setStages(new PipelineStage[] {mlp});

        return pip;
    }

    private static MultilayerPerceptronClassifier load(MultilayerPerceptronClassifier mlp) {
        try {
            FileInputStream f = new FileInputStream("velib.w");
            if(f!=null){
                String ss[]=new Scanner(f).useDelimiter("\\Z").next().split(";");
                double ld[]= new double[ss.length];
                for(int i=0;i<ss.length;i++)ld[i]= Double.parseDouble(ss[i]);
                mlp.setInitialWeights(new DenseVector(ld).asML());
                f.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mlp;
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


    public static String evaluate(PipelineModel model, Dataset<Row> test){
        Dataset<Row> result = model.transform(test);
        Dataset<Row> predictionAndLabels = result.select("prediction", "nPlace");

        String rc="";

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        rc+="Confusion matrix: \n" + toHTML(confusion);

        // Overall statistics
        rc+="Accuracy = " + metrics.accuracy();

        // Stats by labels
        for (int i = 0; i < metrics.labels().length; i++) {
            rc+=String.format("Class %f precision = %f\n", metrics.labels()[i],metrics.precision(metrics.labels()[i]));
            rc+=String.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
            rc+=String.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(metrics.labels()[i]));
        }

        //Weighted stats
        rc+=String.format("Weighted precision = %f\n", metrics.weightedPrecision());
        rc+=String.format("Weighted recall = %f\n", metrics.weightedRecall());
        rc+=String.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
        rc+=String.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());

        return rc;
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

        File f=new File(path);
        String content="";
        if(!f.exists()){
            for(int server=0;server<10;server++){
                String url="https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop."+sDate+"0"+server+".csv";
                Response r=ClientBuilder.newClient().target(url).request(MediaType.TEXT_PLAIN).get();
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
    public static Pipeline trainer=null;

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

    public static String showWeights(PipelineModel model){
        MultilayerPerceptronClassificationModel mpc_model = (MultilayerPerceptronClassificationModel) model.stages()[0];
        return mpc_model.weights().toString();
    }

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException {

        logger.info("Ouverture du port 9999");
        Spark.port(9999);

        logger.info("Creation d'un certificat pour la navigation https");
        createCertificate();

        logger.info("Lancement de l'environnement spark");
        spark=SparkSession.builder().master("local").appName("Java Spark SQL basic example").getOrCreate();
        trainer=createPipeline();
        createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",null));

        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    //String url="https://api.jcdecaux.com/vls/v1/stations/{station_number}?contract=030b9a75c4671dad26255107f2c93dbf710f7bdc";
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    //if(horaire.endsWith("0") || horaire.endsWith("5")){
                        String copyPath="velib_"+System.currentTimeMillis()+".json";
                        train(trainer,createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",copyPath)));
                    //}
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);

        //test : http://localhost:9999/use/PARADIS/0/0
        Spark.get("/use/:station/:delay/:soleil", (request, response) -> {
            String html="";
            if(stations.size()==0)html="Aucune station";

            MultilayerPerceptronClassificationModel model=
                    (MultilayerPerceptronClassificationModel) createPipeline()
                            .fit(createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json",null)))
                            .stages()[0];

            for(Station s:stations)
                if(s.getName().indexOf(request.params("station"))>0){
                    Long delay= Long.valueOf(request.params("delay"));
                    Long date=System.currentTimeMillis()+delay*1000*60;
                    Integer hour=new Date(date).getHours();
                    Integer minutes=new Date(date).getMinutes();
                    Integer day=new Date(date).getDay();
                    Station station=new Station(s.getId(),s.getName(),day,hour,minutes,Double.valueOf(request.params("soleil")));
                    if(model!=null) {
                        org.apache.spark.ml.linalg.Vector v = station.toVector().toDense();
                        station.nPlace=  model.predict(v);
                    } else {
                        station.nPlace=s.getnPlace();
                    }
                    html+=station.toHTML();
                }
            return html;
        });


        Spark.get("/evaluate", (request, response) -> {
            Dataset<Row> dataset = createTrain(getData("https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json", null));
            return evaluate(train(createPipeline(),dataset),dataset);
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
            return evaluate(train(createPipeline(),dataset),dataset);
        });

        Spark.get("/weights", (request, response) -> {
            return showWeights(createPipeline().fit(spark.emptyDataFrame()));
        });
    }



}
