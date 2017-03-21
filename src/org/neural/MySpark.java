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
import java.text.ParseException;
import java.util.*;
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
    private int[] layers=null;
    private Boolean onTraining=false;


    public MultilayerPerceptronClassifier createPerceptron(Integer maxIter, int[] layers) throws IOException {
        if(layers==null)layers=this.layers;

        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier().setLabelCol("label");
        mlp.setLayers(layers);
        mlp.setMaxIter(maxIter);
        mlp.setBlockSize(512);

        mlp=load(mlp, (int[]) layers);
        this.crossValidator=null;

        return mlp;
    }



    public MultilayerPerceptronClassifier createPerceptron(Integer maxIter, Collection<int[]> layers) throws IOException {
        // create the trainer and set its parameters
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier().setLabelCol("label").setBlockSize(512);
        if(new File("./velib.model").exists())mlp=MultilayerPerceptronClassifier.load("./velib.model");

        //if(mlp.getInitialWeights()!=null)logger.warning("weight "+mlp.getInitialWeights().toString());

        Pipeline pipeline=new Pipeline().setStages(new PipelineStage[] {mlp});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(mlp.layers(),JavaConversions.asScalaIterable(layers))
                .addGrid(mlp.maxIter(), new int[] {maxIter})
                .addGrid(mlp.seed(), new long[] {1234L})
                .build();

        crossValidator = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new MulticlassClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(3);

       return mlp;
    }

    public MySpark(String s,Integer nThreads) {
        this.spark=builder().master("local["+nThreads+"]").appName(s).getOrCreate();
        this.spark.sparkContext().setLogLevel("WARN");
    }


    public void save(MultilayerPerceptronClassificationModel model) throws IOException {
        String path="./files/mlp-";
        for(int i:model.layers())path+=String.valueOf(i)+"-";
        FileOutputStream f=new FileOutputStream(path+".weights");
        String s="";
        for(Double d:model.weights().toArray())s+=d+";";
        f.write(s.getBytes());
        f.close();
        model.write().overwrite().save("mlp.model");
    }


    public MultilayerPerceptronClassifier load(MultilayerPerceptronClassifier mlp,int[] layers) {
        try {
            String path="./files/mlp-";
            for(int i:layers)path+=String.valueOf(i)+"-";
            logger.info("Chargement des poids de "+path);
            FileInputStream f = new FileInputStream(path+".weights");
            if(f!=null){
                String ss[]=new Scanner(f).useDelimiter("\\Z").next().split(";");
                double ld[]= new double[ss.length];
                for(int i=0;i<ss.length;i++)ld[i]= Double.parseDouble(ss[i]);

                DenseVector ws = new DenseVector(ld);
                mlp.setInitialWeights(ws.asML());
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


    public void initModel() throws IOException {
        MultilayerPerceptronClassifier mlp = createPerceptron(0, (int[]) null);
    }

    public String findModel(Datas datas,Integer iter,Collection<int[]> layers) throws IOException {
        Dataset<Row>[] dts = datas.createTrain().randomSplit(new double[]{0.6, 0.4});
        createPerceptron(iter,layers);
        PipelineModel pm= (PipelineModel) crossValidator.fit(dts[0].cache()).bestModel();
        this.model= (MultilayerPerceptronClassificationModel) pm.stages()[0] ;
        save(this.model);
        this.layers=this.model.layers();

        return evaluate(dts[1]);
    }

    public String train(Datas datas,Integer iter,Collection<int[]> layers) throws IOException {
        Dataset<Row>[] dts = datas.createTrain().randomSplit(new double[]{0.6, 0.4});
        createPerceptron(iter,layers);

        this.onTraining=true;
        PipelineModel pm= (PipelineModel) crossValidator.fit(dts[0].cache()).bestModel();

        this.model= (MultilayerPerceptronClassificationModel) pm.stages()[0] ;
        save(this.model);

        this.onTraining=false;

        return evaluate(dts[1]);
    }

    public String train(Datas datas,Integer iter,int[] layer) throws IOException {
        Dataset<Row>[] dts = datas.createTrain().randomSplit(new double[]{0.6, 0.4});
        MultilayerPerceptronClassifier mlp = createPerceptron(iter, layer);

        this.onTraining=true;
        this.model=mlp.fit(dts[0]);
        this.onTraining=false;

        save(this.model);
        String rc=evaluate(dts[1]);
        logger.warning("Evaluation "+rc);
        return rc;
    }

    public String showWeights() throws IOException {
        return showWeights(this.model);
    }


    public String showWeights(MultilayerPerceptronClassificationModel model) throws IOException {
        if(model==null)return "you must build a model before ask weights";
        return model.weights().toString().replaceAll(",","<br>");
    }


    public Dataset<Row> createTrain(String file){
        return spark.read().json(file);
    }


    public String evaluate(Dataset<Row> r) throws IOException {
        Dataset<Row> result = this.model.transform(r);
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        predictionAndLabels.show(30,false);
        String rc="";
        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();

        Date now=new Date(System.currentTimeMillis());
        rc+="<h1>Iteration a "+now.getHours()+":"+now.getMinutes()+"</h1>";
        rc+="<br>Confusion matrix: \n" + Tools.toHTML(confusion)+"<br><br>";

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



    public String predict(Datas d,int[] layer) throws IOException, ParseException {
        if(this.model==null)this.model= createPerceptron(0,layer).fit(d.getData());
        Dataset<Row> a = this.model.transform(d.createTrain());
        return a.showString(1,100);
    }


    public String predictDetail(Vector v) throws IOException {
        Row r= RowFactory.create(v.toArray());
        Dataset<Row> f=spark.emptyDataFrame();
        return this.model.transform(f).toString();
    }


    public String evaluate(Datas stations) throws IOException {
        if(this.model==null)this.model=MultilayerPerceptronClassificationModel.load("mlp.model");
        return this.evaluate(stations.createTrain().randomSplit(new double[]{0.3,0.6})[1]);
    }

    public String showLayers() {
        String rc="";
        for(int i:this.layers)rc+=i+"-";
        return rc;
    }

    public Dataset<Row> use(Dataset<Row> input,int[] layer) throws IOException {
        input.show(20,false);
        if(this.model==null)this.model=createPerceptron(0,layer).fit(input);
        Dataset<Row> rc=this.model.transform(input);
        rc.show(20,false);
        return rc;
    }

    public boolean onTrain() {
        return this.onTraining;
    }
}