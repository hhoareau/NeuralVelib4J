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

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;
import java.util.logging.Logger;

import static org.apache.spark.sql.SparkSession.builder;

/**
 * Created by u016272 on 23/02/2017.
 */
public class MySpark {

    //private Pipeline pipeline=null;
    private SparkSession spark=null;
    private MultilayerPerceptronClassificationModel model=null;
    private CrossValidator crossValidator=null;
    private static Logger logger = Logger.getLogger(String.valueOf(MySpark.class));


    public void createPipeline(Integer maxIter)  {
        // create the trainer and set its parameters
        int inputLayer=new Station().colsName().length;
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier()
                .setLabelCol("label");

        Collection<int[]> layers = new HashSet<>();
        layers.add(new int[]{inputLayer, 25, 20, 4});
        //layers.add(new int[]{inputLayer,50,4});
        //layers.add(new int[]{inputLayer,7,7,7,4});
        //layers.add(new int[]{inputLayer,50,50,4});

        load(mlp, (int[]) layers.toArray()[0]);
        //if(mlp.getInitialWeights()!=null)logger.warning("weight "+mlp.getInitialWeights().toString());

        Pipeline pipeline=new Pipeline().setStages(new PipelineStage[] {mlp});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(mlp.blockSize(), new int[] {128})
                .addGrid(mlp.layers(),JavaConversions.asScalaIterable(layers))
                .addGrid(mlp.maxIter(), new int[] {maxIter})
                .addGrid(mlp.seed(), new long[] {1234L})
                .build();

        crossValidator = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new MulticlassClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(2);  // Use 3+ in practice
    }

    public MySpark(String s) {
        this.spark=builder().master("local").appName(s).getOrCreate();
        this.spark.sparkContext().setLogLevel("WARN");
        createPipeline(10);
    }

    public void save(CrossValidatorModel model) throws IOException {
        PipelineModel pm = (PipelineModel) model.bestModel();
        this.model= (MultilayerPerceptronClassificationModel) pm.stages()[0];

        String path="./files/mlp-";
        for(int i:this.model.layers())path+=String.valueOf(i)+"-";

        FileOutputStream f=new FileOutputStream(path+".weights");
        String s="";
        for(Double d:this.model.weights().toArray())s+=d+";";
        f.write(s.getBytes());
        f.close();

    }

    public MultilayerPerceptronClassifier load(MultilayerPerceptronClassifier mlp,int[] layers) {
        try {
            String path="./files/mlp-";
            for(int i:layers)path+=String.valueOf(i)+"-";

            FileInputStream f = new FileInputStream(path+".weights");
            if(f!=null){
                String ss[]=new Scanner(f).useDelimiter("\\Z").next().split(";");
                double ld[]= new double[ss.length];
                for(int i=0;i<ss.length;i++)ld[i]= Double.parseDouble(ss[i]);

                mlp.setInitialWeights(new DenseVector(ld).asML());
                f.close();
            }
        } catch (FileNotFoundException e) {
            //mlp.setInitialWeights(Vectors.zeros(new Station().colsName().length));
            //e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        }
        return mlp;
    }


    public String train(Datas datas,Integer iter) throws IOException {
        Dataset<Row> r=datas.createTrain();
        Dataset<Row>[] dts = r.randomSplit(new double[]{0.7, 0.3});
        createPipeline(iter);
        CrossValidatorModel model=null;
        try{
            model= crossValidator.fit(dts[0]);
        }catch (IllegalArgumentException e){
            new File("./files/velib.w").delete();
        }

        PipelineModel pm= (PipelineModel) model.bestModel();
        this.model= (MultilayerPerceptronClassificationModel) pm.stages()[0] ;
        save(model);
        return evaluate(dts[1]);
    }

    public String showWeights() throws IOException {
        if(this.model==null)return "you must build a model before ask weights";
        return this.model.weights().toString().replaceAll(",","<br>");
    }


    public Dataset<Row> createTrain(String file){
        return spark.read().json(file);
    }

    public String evaluate(Dataset<Row> r) throws IOException {

        Dataset<Row> result = model.transform(r);
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        predictionAndLabels.show(30,false);
        String rc="";

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();

        rc+="Confusion matrix: \n" + Tools.toHTML(confusion)+"<br><br>";

        // Overall statistics
        rc+="Accuracy = " + metrics.accuracy()+"<br>";

        rc+="Recall : "+metrics.recall();

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



    public String predict(Station s) throws IOException {
        Dataset<Row> dt = spark.createDataFrame(Arrays.asList(s), Station.class);
        dt=this.model.transform(Tools.createTrain(dt));
        return dt.showString(1,100);
    }

    public String predictDetail(Vector v) throws IOException {
        Row r= RowFactory.create(v.toArray());
        Dataset<Row> f=spark.emptyDataFrame();
        return this.model.transform(f).toString();
    }

    public String evaluate(Datas stations) throws IOException {
        if(model==null)train(stations,1);
        return this.evaluate(stations.createTrain().randomSplit(new double[]{0.3,0.6})[0]);
    }

}