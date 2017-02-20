import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.RowFactory;
import org.codehaus.jackson.JsonNode;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Hervé on 20/02/2017.
 */
public class Station implements Serializable {
    Double lt=0.0;
    Double lg=0.0;
    Integer temperature=0;
    Long id=null;
    Integer month=0;
    Integer minute=0;
    Integer day=0;
    Integer hour=0;
    Integer nPlace=0;

    public Station(Double lt, Double lg, Integer temperature, Long id, Integer month, Integer minute, Integer day) {
        this.lt = lt;
        this.lg = lg;
        this.temperature = temperature;
        this.id = id;
        this.month = month;
        this.minute = minute;
        this.day = day;
    }

    public Station(JsonNode jnode, Integer temperature, Long dt){
        this.nPlace=jnode.get("available_bike_stands").asInt();
        this.lt= Double.valueOf(jnode.get("position").get(0).asDouble());
        this.lg= Double.valueOf(jnode.get("position").get(1).asDouble());
        this.id=jnode.get("number").asLong();
        this.temperature=temperature;
        this.day=new Date(dt).getDay();
        this.minute=new Date(dt).getMinutes();
        this.month=new Date(dt).getMonth();
        this.hour=new Date(dt).getHours();
        //Row rc= RowFactory.create(nPlace, Vectors.dense(id, position, temperature,0.1,new Date(dt).getHours(),,new Date().getMinutes()));
    }

    public org.apache.spark.sql.Row toRow(){
        return RowFactory.create(this.nPlace, Vectors.dense(id, this.lt,this.lg, this.temperature,0.1,this.day,this.month,this.hour,this.minute));
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Integer getnPlace() {
        return nPlace;
    }

    public void setnPlace(Integer nPlace) {
        this.nPlace = nPlace;
    }

    public Double getLt() {
        return lt;
    }

    public void setLt(Double lt) {
        this.lt = lt;
    }

    public Double getLg() {
        return lg;
    }

    public void setLg(Double lg) {
        this.lg = lg;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getMinute() {
        return minute;
    }

    public void setMinute(Integer minute) {
        this.minute = minute;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }
}
