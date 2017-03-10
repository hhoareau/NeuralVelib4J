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


    public MultilayerPerceptronClassifier createPipeline(Integer maxIter) throws IOException {
        // create the trainer and set its parameters
        int inputLayer=new Station().colsName().length;

        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier().setLabelCol("label");
        if(new File("./velib.model").exists())
            mlp=MultilayerPerceptronClassifier.load("./velib.model");

        Collection<int[]> layers = new HashSet<>();
        layers.add(new int[]{inputLayer, 15, 20, 4});
        //layers.add(new int[]{inputLayer,50,4});
        //layers.add(new int[]{inputLayer,7,7,7,4});
        //layers.add(new int[]{inputLayer,50,50,4});

        mlp=load(mlp, (int[]) layers.toArray()[0]);

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
                .setEstimatorParamMaps(paramGrid).setNumFolds(3);

       return mlp;
    }

    public MySpark(String s) throws IOException {
        this.spark=builder().master("local[4]").appName(s).getOrCreate();
        this.spark.sparkContext().setLogLevel("WARN");
        createPipeline(10);
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


    public void initModel() throws IOException {
        MultilayerPerceptronClassifier mlp = createPipeline(0);

    }

    public String train(Datas datas,Integer iter) throws IOException {
        Dataset<Row>[] dts = datas.createTrain().randomSplit(new double[]{0.5, 0.5});
        createPipeline(iter);

        PipelineModel pm= (PipelineModel) crossValidator.fit(dts[0].cache()).bestModel();

        this.model= (MultilayerPerceptronClassificationModel) pm.stages()[0] ;
        save(this.model);

        return evaluate(dts[1]);
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

        Dataset<Row> result = model.transform(r);
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



    public String predict(Datas d) throws IOException, ParseException {
        if(this.model==null)
            this.model=MultilayerPerceptronClassificationModel.load("mlp.model");

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
        return this.evaluate(stations.createTrain().randomSplit(new double[]{0.3,0.6})[0]);
    }

}