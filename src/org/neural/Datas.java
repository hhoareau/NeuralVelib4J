package org.neural;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Datas {

    private static Logger logger = Logger.getLogger(String.valueOf(Datas.class));

    //private List<Station> stations=new ArrayList<>();
    private Dataset<Row> df=null;
    private Long size=0L;

    public Datas() {
        size=0L;
    }

    public Datas(SparkSession spark,String path,String filter) throws IOException, ParseException {
        logger.setLevel(Level.INFO);
        List<Station> stats=new ArrayList<>();
        for (File f : new File(path).listFiles())
            if(f.getName().indexOf(".json")>0){
                logger.info("Chargement de "+f.getName());
                stats.addAll(Tools.getStations(Tools.getData(f.getAbsolutePath(), null), 1.0,filter));
                if(stats.size()>100000){
                    add(stats, spark);
                    size+=stats.size();
                    df=df.distinct();
                    df.persist();
                    stats.clear();
                }
            }

        add(stats, spark);
        size+=stats.size();
        df=df.distinct();
        df.persist();
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
        size=0L;
    }

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

    public String showData() {
        return Tools.DatasetToHTML(this.df.showString(200,100));
    }

    public Dataset<Row> getData() {
        return this.df;
    }
}
