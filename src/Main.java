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

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Main {

    static SparkSession spark=null;

    public static void train(MultilayerPerceptronClassifier trainer,Dataset<Row> stations){
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"id","lt","lg","day", "hour","month","minute","temperature"})
                .setOutputCol("features");
        Dataset<Row> rows=assembler.transform(stations);
        Dataset<Row>[] splits = rows.randomSplit(new double[]{0.6, 0.4},1234L);

        MultilayerPerceptronClassificationModel model = trainer.fit(rows);
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


    public static JsonNode getData(String str)  {
        try {
            if(str.startsWith("http")){
                Response r=ClientBuilder.newClient().target(str).request(MediaType.APPLICATION_JSON).get();
                Integer code=r.getStatus();
                if(code==200){
                    return new ObjectMapper().readTree(new InputStreamReader(r.readEntity(InputStream.class)));
                }
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

        Dataset<Row> rc=spark.createDataFrame(stations,Station.class);
        return rc.persist();
    }


    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    public static MultilayerPerceptronClassifier trainer=null;

    public static void main(String[] args) throws IOException {
        Spark.port(8080);

        // create the trainer and set its parameters
        trainer = new MultilayerPerceptronClassifier()
                .setLayers(new int[] {8, 5, 4, 3})
                .setBlockSize(128)
                .setSeed(1234L)
                .setLabelCol("nPlace")
                .setMaxIter(1000);


        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    train(trainer,createTrain(getData("stations-velib-disponibilites-en-temps-reel.json")));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,5, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);


        spark= SparkSession.builder().master("local").appName("Java Spark SQL basic example").getOrCreate();

        Spark.get("/getdata", (request, response) -> {
            return "ok";
        });

        Spark.get("/init", (request, response) -> {
            return "ok";
        });
    }

}
