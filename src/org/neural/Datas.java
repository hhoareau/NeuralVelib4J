package org.neural;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.JsonNode;
import scala.Array;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Datas {

    private static Logger logger = Logger.getLogger(String.valueOf(Datas.class));

    private List<Station> stations=new ArrayList<>();
    private JavaRDD<Station> rdd_stations=null;

    public Datas() {
    }

    public void initList(JsonNode jsonNode,Double temperature,String filter) throws ParseException {
        if(jsonNode!=null){
            //this.rdd_stations=spark.sparkContext().parallelize(stations.toArray());

            Iterator<JsonNode> ite=jsonNode.getElements();
            if(ite!=null)
                while(ite.hasNext()){
                    JsonNode item=ite.next().get("fields");
                    if(item!=null && item.has("status") && item.get("status").asText().equals("OPEN")){
                        Station s=new Station(item, temperature);
                        if(filter==null || s.getName().indexOf(filter)>-1)
                            this.add(s);
                    }
                }
        }
    }


    public Datas(String path,String filter) throws IOException, ParseException {

        Map<String,Double> data=Tools.getMeteo(new Date(System.currentTimeMillis()));
        initList(Tools.getData(path,"./files/velib_"+System.currentTimeMillis()+".json"),data.get("temperature"),filter);
    }



    public Datas(Double part,String filter) throws IOException, ParseException {
        File dir=new File("./files/");
        Double nb=dir.listFiles().length*part;
        for(File f:dir.listFiles()){
            if(f.getName().indexOf(".json")>0 && nb>0){
                initList(Tools.getDataFromFile("./files/"+f.getName(),null),1.0,filter);
                nb--;
            }
        }
    }

    public String toHTML(Integer max){
        String html="";
        if(stations.size()==0)return "no stations";

        Collections.sort(stations);

        for(Station s:this.stations){
            html+=s.toHTML()+"<br>";
            if(max--<0)break;
        }


        return html;
    }

    public Dataset<Row> createTrain(SparkSession spark) throws IOException {
        Dataset<Row> rc=spark.createDataFrame(stations,Station.class);

        rc.persist(StorageLevel.MEMORY_ONLY());

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new Station().colsName())
                .setOutputCol("tempFeatures");
        rc=assembler.transform(rc);

        rc=rc.drop(new String[]{"lg","lt","name","dtUpdate","day","hour","id","minute","soleil"});

        rc.show(600,false);

        Normalizer normalizer = new Normalizer()
                .setInputCol("tempFeatures")
                .setOutputCol("features")
                .setP(1.0);
        rc=normalizer.transform(rc);

        return rc;
    }

    public Iterator<Station> getIterator(){
        return this.stations.iterator();
    }

    public Integer getSize(){
        return this.stations.size();
    }

    public Station[] getStations() {
        return this.getStations();
    }

    public void add(Datas datas) {
        this.stations.addAll(datas.stations);
    }

    public void add(Station s) {
        //Encoder<Station> e= Encoders.bean(Station.class);
        if(!this.stations.contains(s))this.stations.add(s);
        //else logger.info(s.toString()+" en doublon");
    }

    public Station getStation(String name) {
        Iterator<Station> ite = this.getIterator();
        while (ite.hasNext()) {
            Station s = ite.next();
            if (s.getName().indexOf(name.toUpperCase()) > 0) {
                return s;
            }
        }
        return null;
    }
}
