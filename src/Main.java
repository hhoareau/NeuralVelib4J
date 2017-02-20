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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Main {

    static SparkSession spark=null;


    public static void init(Dataset<Row> stations){


        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"id","day", "hour","month","minute","temperature"})
                .setOutputCol("features");

        Dataset<Row> rows=assembler.transform(stations);
        rows=rows.drop(new String[]{"id","day", "hour", "lg","lt","month","minute","temperature"});
        rows.show();

        Dataset<Row>[] splits = rows.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

// specify layers for the neural network:
// input layer of size 4 (features), two intermediate of size 5 and 4
// and output of size 3 (classes)
        int[] layers = new int[] {6, 5, 4, 1};

// create the trainer and set its parameters
        MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setLabelCol("nPlace")
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(100);

// train the model
        MultilayerPerceptronClassificationModel model = trainer.fit(train);

// compute accuracy on the test set
        Dataset<Row> result = model.transform(test);
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");

    }

    public static JsonNode getData(String str)  {
        try {
            if(str.startsWith("http")){
                Client client = ClientBuilder.newClient();
                WebTarget target = client.target(str);
                Response r=target.request(MediaType.APPLICATION_JSON).get();
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

    public static Dataset<Row> createTrain(JsonNode jsonNode){
        List<Station> stations= new ArrayList<>();

        Iterator<JsonNode> ite=jsonNode.getElements();
        while(ite.hasNext())
            stations.add(new Station(ite.next().get("fields"),10, System.currentTimeMillis()));

        return spark.createDataFrame(stations,Station.class);
    }

    public static void main(String[] args) {
        Spark.port(8080);
        spark= SparkSession.builder().master("local").appName("Java Spark SQL basic example").getOrCreate();

        init(createTrain(getData("stations-velib-disponibilites-en-temps-reel.json")));

        Spark.get("/getdata", (request, response) -> {
            return "ok";
        });

        Spark.get("/init", (request, response) -> {
            return "ok";
        });
    }

}
