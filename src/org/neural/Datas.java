package org.neural;

import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Datas {

    private static Logger logger = Logger.getLogger(String.valueOf(Datas.class));

    private Dataset<Row> df=null;
    private Long size=0L;

    public Datas(SparkSession spark,String path,String filter) throws IOException, ParseException {
        logger.setLevel(Level.INFO);
        List<Station> stats=new ArrayList<>();
        for (File f : new File(path).listFiles())
            if(f.getName().indexOf(".json")>0){
                logger.warning("Chargement de "+f.getName());
                List<Station> temp = Tools.getStations(Tools.getData(f.getAbsolutePath(), null), 1.0, filter);
                if(temp!=null){
                    stats.addAll(temp);
                    if(stats.size()>100000){
                        add(stats, spark);
                        size+=stats.size();
                        df=df.distinct();
                        df.persist();
                        stats.clear();
                    }
                }
            }

        add(stats, spark);
        size+=stats.size();
        df=df.distinct();
        df.persist();
    }

    public Datas(SparkSession spark) {
        logger.setLevel(Level.INFO);
        this.df=null;
        this.size=0L;
    }

    public Datas(SparkSession spark,Station s) throws IOException, ParseException {
        logger.setLevel(Level.INFO);
        this.size=0L;
        this.add(Arrays.asList(s),spark);
    }

    public Dataset<Row> createTrain() throws IOException {
        //df.show(30,false);
        Dataset<Row> rc=df.drop(new String[]{"lg","lt","dtUpdate","nPlace","nBike"});

        rc = new StringIndexer().setInputCol("id").setOutputCol("id_index").fit(rc).transform(rc);
        rc.show(30,false);

        /*
        Dataset<Row> encoded1= new OneHotEncoder().setInputCol("hour").setOutputCol("hourVect").transform(indexed);
        Dataset<Row> encoded2 = new OneHotEncoder().setInputCol("id").setOutputCol("idVect").transform(encoded1);
        Dataset<Row> encoded3 = new OneHotEncoder().setInputCol("soleil").setOutputCol("soleilVect").transform(encoded2);
        Dataset<Row> encoded4 = new OneHotEncoder().setInputCol("day").setOutputCol("dayVect").transform(encoded3);
        */

        rc=new VectorAssembler().setInputCols(new String[]{"hour","minute","id_index","soleil","day"}).setOutputCol("tempFeatures").transform(rc);

        rc=new Normalizer().setInputCol("tempFeatures").setOutputCol("features").transform(rc);

        rc=rc.drop(new Station().colsName()).drop("tempFeatures");

        rc.show(30,false);

        return rc;
    }

    public Datas(Double part,String filter) throws IOException, ParseException {
        File dir=new File("./files/");
        Double nb=dir.listFiles().length*part;
        for(File f:dir.listFiles()){
            if(f.getName().indexOf(".json")>0 && nb>0){
                //initList(Tools.getDataFromFile("./files/"+f.getName(),null),1.0,filter);
                nb--;
            }
        }
    }

    /**
     *
     * @param stations
     * @param spark
     * @throws IOException
     * @throws ParseException
     */
    public void add(List<Station> stations,SparkSession spark) throws IOException, ParseException {
        int limit=3;
        int nStations=stations.size()/limit;
        if(nStations<3){nStations=stations.size();limit=1;}
        for(int i=0;i<limit;i++){
            Dataset<Row> rs=spark.createDataFrame(stations.subList(i*nStations,Math.min((i+1)*nStations,stations.size())),Station.class);
            if(df==null)
                df=rs;
            else
                df=df.union(rs);
        }
        this.size+=stations.size();
        df.persist(StorageLevel.MEMORY_ONLY());
    }


    public String toHTML(Integer max){
        String html="";
        for(Row r:this.df.collectAsList()){
            html+=new Station(r).toHTML()+"<br>";
            if(max--<0)break;
        }
        return html;
    }


    public Station getStation(String name) {
        if(this.df==null)return null;
        List<Row> lr=this.df.filter(col("name").contains(name)).collectAsList();
        if(lr.size()>0)
            return new Station(lr.get(0));
        else
            return null;
    }

    public long getSize() {
        return size;
    }

    public void save() throws FileNotFoundException  {
        String path="./files/stations";
        //FileOutputStream f=new FileOutputStream(path);
        //this.df.toJSON().write().csv(path);
    }

    public void load(SparkSession spark)  {
        FileInputStream f=null;
        try {
            f=new FileInputStream("./files/stations.csv");
        } catch (FileNotFoundException e) {
        }
        if(f!=null)this.df=spark.readStream().schema(this.df.schema()).load();
    }

    public String showData(Integer line) {
        return Tools.DatasetToHTML(this.df.showString(line,100));
    }

    public Dataset<Row> getData() {return this.df;}

    public String toCSV(MySpark spark,String sepCol,String sepLine,int[] layer) throws IOException {
        Dataset<Row> result = spark.use(this.createTrain());

        result.show(20,false);

        Dataset<Row> toExport=result;
        toExport.show(20,false);

        return Tools.toCSV(this.getData(),";","\n");
    }
}
