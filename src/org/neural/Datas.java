package org.neural;

import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Datas {

    private static Logger logger = Logger.getLogger(String.valueOf(Datas.class));

    //private List<Station> stations=new ArrayList<>();
    private Dataset<Row> df=null;

    public Datas() {
    }

    public Datas(SparkSession spark,String path,String filter) throws IOException, ParseException {
        for (File f : new File(path).listFiles())
            if(f.getName().indexOf(".json")>0){
                logger.info("Chargement de "+f.getName());
                add(Tools.getStations(Tools.getData(f.getAbsolutePath(), null), 1.0,filter), spark);
                df=df.distinct();
            }
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

    public Datas(MySpark spark) throws FileNotFoundException {
        this.load(spark.getSession());
    }

    public void add(List<Station> stations,SparkSession spark) throws IOException, ParseException {
        Dataset<Row> rs=spark.createDataFrame(stations,Station.class);
        if(df==null)
            df=rs;
        else
            df=df.union(rs);
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

    public Dataset<Row> createTrain(SparkSession spark) throws IOException {
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new Station().colsName())
                .setOutputCol("tempFeatures");

        Dataset<Row> rc=assembler.transform(this.df);

        rc=rc.drop(new String[]{"lg","lt","name","dtUpdate","day","hour","id","minute","soleil"});

        rc.show(600,false);

        Normalizer normalizer = new Normalizer()
                .setInputCol("tempFeatures")
                .setOutputCol("features")
                .setP(1.0);
        rc=normalizer.transform(rc);

        return rc;
    }

    public Station getStation(String name) {
        List<Row> lr=this.df.filter("name='"+name+"'").collectAsList();
        if(lr.size()>0)
            return new Station(lr.get(0));
        else
            return null;
    }

    public int getSize() {
        if(df==null)return 0;
        return this.df.collectAsList().size();
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
            e.printStackTrace();
        }
        if(f!=null)this.df=spark.readStream().schema(this.df.schema()).load();
    }

}
