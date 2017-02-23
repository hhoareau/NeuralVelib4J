package org.neural;

import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.JsonNode;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by u016272 on 23/02/2017.
 */
public class Datas {

    private List<Station> stations=new ArrayList<>();

    public Datas() {
    }

    public void initList(JsonNode jsonNode,Double temperature){
        Iterator<JsonNode> ite=jsonNode.getElements();
        while(ite.hasNext())stations.add(new Station(ite.next().get("fields"), temperature, System.currentTimeMillis()));
    }


    public Datas(String path) throws IOException {
        Map<String,Double> data=Tools.getMeteo(new Date(System.currentTimeMillis()));
        initList(Tools.getData(path,"./files/velib_"+System.currentTimeMillis()+".json"),data.get("temperature"));
    }


    public Datas(Double part) throws IOException {
        File dir=new File("./files/");
        Double nb=dir.listFiles().length*part;
        for(File f:dir.listFiles()){
            if(f.getName().indexOf(".json")>0 && nb>0){
                initList(Tools.getDataFromFile("./files/"+f.getName(),null),1.0);
                nb--;
            }
        }
    }

    public String toHTML(){
        String html="";
        if(stations.size()==0)return "no stations";
        for(Station s:this.stations)
            html+=s.toHTML()+"<br>";

        return html;
    }


    public Dataset<Row> createTrain(SparkSession spark) throws IOException {
        Dataset<Row> rc=spark.createDataFrame(this.stations, Station.class);
        rc.persist();

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new Station().colsName())
                .setOutputCol("tempFeatures");
        rc=assembler.transform(rc);

        Normalizer normalizer = new Normalizer()
                .setInputCol("tempFeatures")
                .setOutputCol("features")
                .setP(1.0);
        rc=normalizer.transform(rc);

        return rc;
    }

    public Integer getSize(){
        return this.stations.size();
    }

    public Station[] getStations() {
        return this.getStations();
    }
}
