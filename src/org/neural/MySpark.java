package org.neural;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.JsonNode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.SparkSession.*;

/**
 * Created by u016272 on 23/02/2017.
 */
public class MySpark {

    private Pipeline pipeline=null;
    private SparkSession spark=null;
    private PipelineModel model=null;

    public void createPipeline(Integer iter)  {
        // create the trainer and set its parameters
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier()
                .setLayers(new int[] {new Station().colsName().length,50,50,11})
                .setBlockSize(128)
                .setSeed(1234L)
                .setLabelCol("nPlace")
                .setMaxIter(iter);

        load(mlp);

        pipeline=new Pipeline().setStages(new PipelineStage[] {mlp});
    }

    public MySpark(String s) {
        this.spark=builder().master("local").appName("Java Spark SQL basic example").getOrCreate();
        createPipeline(100);
    }

    public void save(MultilayerPerceptronClassificationModel mlp_model) throws IOException {
        org.apache.spark.ml.linalg.Vector v = mlp_model.weights();
        FileOutputStream f=new FileOutputStream("./files/velib.w");
        String s="";
        for(Double d:v.toArray())s+=d+";";
        f.write(s.getBytes());
        f.close();
    }

    public MultilayerPerceptronClassifier load(MultilayerPerceptronClassifier mlp) {
        try {
            FileInputStream f = new FileInputStream("./files/velib.w");
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

    public MultilayerPerceptronClassificationModel getMLPmodel() throws IOException {
        if(model==null)train(new Datas(),1);
        if(model.stages().length>0)
            return (MultilayerPerceptronClassificationModel)model.stages()[0];
        else
            return null;
    }

    public void train(Datas datas,Integer iter) throws IOException {
        Dataset<Row> r=datas.createTrain(this.getSession());
        createPipeline(iter);
        this.model=pipeline.fit(r);
        save(this.getMLPmodel());
    }


    public String showWeights() throws IOException {
        return this.getMLPmodel().weights().toString();
    }


    public Dataset<Row> createTrain(String file){
        return spark.read().json(file);
    }

    public String evaluate(Dataset<Row> r) throws IOException {
        Dataset<Row> result = model.transform(r);
        Dataset<Row> predictionAndLabels = result.select("prediction", "nPlace");

        String rc="";

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();

        rc+="Confusion matrix: \n" + Tools.toHTML(confusion)+"<br><br>";

        // Overall statistics
        rc+="Accuracy = " + metrics.accuracy()+"<br>";

        // Stats by labels
        for (int i = 0; i < metrics.labels().length; i++)
            rc+=String.format("<br>Class %f precision = %f / recall = %f / score = %f", metrics.labels()[i],metrics.precision(metrics.labels()[i]),metrics.recall(metrics.labels()[i]),metrics.fMeasure(metrics.labels()[i]));

        //Weighted stats
        rc+=String.format("<br><br>Weighted precision = %f / recall = %f / F1 score = %f / false positive rate = %f",
                metrics.weightedPrecision(),metrics.weightedFMeasure(),metrics.weightedRecall(),metrics.weightedFalsePositiveRate());

        return rc;
    }


    public SparkSession getSession() {
        return this.spark;
    }

    public Double predict(Vector v) throws IOException {
        return this.getMLPmodel().predict(v);
    }


    public String evaluate(Datas stations) throws IOException {
        if(model==null)train(stations,1);
        return this.evaluate(stations.createTrain(this.getSession()));
    }


}
