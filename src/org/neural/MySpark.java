package org.neural;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;

import static org.apache.spark.sql.SparkSession.builder;

/**
 * Created by u016272 on 23/02/2017.
 */
public class MySpark {

    //private Pipeline pipeline=null;
    private SparkSession spark=null;
    private PipelineModel model=null;
    private CrossValidator crossValidator=null;


    public void createPipeline()  {
        // create the trainer and set its parameters
        int inputLayer=new Station().colsName().length;
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier()
                .setLabelCol("nPlace");

        load(mlp);

        Collection<int[]> layers = new HashSet<>();
        layers.add(new int[]{inputLayer, 5, 5, 11});
        layers.add(new int[]{inputLayer,20,20,11});
        layers.add(new int[]{inputLayer,7,7,7,11});
        layers.add(new int[]{inputLayer,50,50,11});

        Pipeline pipeline=new Pipeline().setStages(new PipelineStage[] {mlp});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(mlp.blockSize(), new int[] {128, 200})
                .addGrid(mlp.layers(),JavaConversions.asScalaIterable(layers))
                .addGrid(mlp.maxIter(), new int[] {10,100,1000})
                .addGrid(mlp.seed(), new long[] {1234L})
                .build();

        crossValidator = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new MulticlassClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(2);  // Use 3+ in practice


    }

    public MySpark(String s) {
        this.spark=builder().master("local").appName(s).getOrCreate();
        this.spark.sparkContext().setLogLevel("INFO");
        createPipeline();
    }

    public void save(CrossValidatorModel model) throws IOException {
        ParamMap p = model.bestModel().extractParamMap();
        /*
        FileOutputStream f=new FileOutputStream("./files/velib.w");
        String s="";
        for(Double d:v.toArray())s+=d+";";
        f.write(s.getBytes());
        f.close();
        */
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

    public String train(Datas datas,Integer iter) throws IOException {
        Dataset<Row> r=datas.createTrain(this.getSession());
        Dataset<Row>[] dts = r.randomSplit(new double[]{0.7, 0.3});
        createPipeline();
        CrossValidatorModel model= crossValidator.fit(dts[0]);
        save(model);
        return evaluate(dts[1]);
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

    public String predictDetail(Vector v) throws IOException {
        Row r= RowFactory.create(v.toArray());
        Dataset<Row> f=spark.emptyDataFrame();
        return this.getMLPmodel().transform(f).toString();
    }

    public String evaluate(Datas stations) throws IOException {
        if(model==null)train(stations,1);
        return this.evaluate(stations.createTrain(this.getSession()).randomSplit(new double[]{0.3,0.6})[0]);
    }

}