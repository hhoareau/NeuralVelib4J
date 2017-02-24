package org.neural;

import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.codehaus.jackson.JsonNode;
import scala.Serializable;

import java.util.Date;

/**
 * Created by Hervé on 20/02/2017.
 */
public class Station implements Serializable {
    Long id=0L;
    String name="";
    Double lt=0.0;
    Double lg=0.0;
    Double soleil=1.0;
    Integer month=0;
    Integer minute=0;
    Integer day=0;
    Integer hour=0;
    Double nPlace=0.0; //0,1 ou 2

    /**
     *
     * @param jnode
     * @param temperature
     * @param dt
     */
    public Station(JsonNode jnode, Double temperature, Long dt){
        this.id=jnode.get("number").asLong();
        this.name=jnode.get("name").asText();

        this.nPlace=jnode.get("available_bike_stands").asDouble();
        if(this.nPlace>0 && this.nPlace<10)this.nPlace=1.0;
        if(this.nPlace>=10)this.nPlace=2.0;

        this.lt= Double.valueOf(jnode.get("position").get(0).asDouble());
        this.lg= Double.valueOf(jnode.get("position").get(1).asDouble());

        if(temperature<15)soleil=0.0; else soleil=1.0;

        this.day=new Date(dt).getDay();
        this.minute=Math.round(new Date(dt).getMinutes()/5)*5;

        this.month=new Date(dt).getMonth();
        this.hour=new Date(dt).getHours();
    }


    public Station(Long station, String name,Integer day, Integer hour, Integer minute,Double soleil) {
        this.id= station;
        this.name=name;
        this.day=day;
        this.hour=hour;
        this.minute=Math.round(minute/5)*5;
        this.soleil=soleil;
    }

    public Station() {}

    public Station(Station s, Long date, Double soleil) {
        this.id=s.getId();
        this.hour=new Date(date).getHours();
        this.minute=new Date(date).getMinutes();
        this.day=new Date(date).getDay();
        this.soleil=s.getSoleil();
        this.name=s.getName();
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public Double getSoleil() {
        return soleil;
    }

    public void setSoleil(Double soleil) {
        this.soleil = soleil;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Double getnPlace() {
        return nPlace;
    }

    public void setnPlace(Double nPlace) {
        Double rc=0.0;
        if(nPlace>0 && nPlace<10)rc=1.0;
        if(nPlace>10)rc=2.0;
        this.nPlace = rc;
    }

    public Vector toVector() {
        DenseVector v=new DenseVector(new double[]{this.id,this.day,this.hour,this.month,this.minute,this.soleil});
        return v;
    }

    public String[] colsName() {
        return new String[]{"id","day","hour","month","minute","soleil"};
    }

    public String toHTML(){
        String html="<h1>"+this.name+"</h1>";
        html+=this.hour+":"+this.minute+"<br>";

        String place="aucun vélo";
        if(this.nPlace==1)place="entre 1 et 10 vélos";
        if(this.nPlace==2)place="plus de 10 vélos";
        html+=place;
        return html;
    }
}
