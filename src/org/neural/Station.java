package org.neural;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.codehaus.jackson.JsonNode;
import scala.Serializable;

import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Hervé on 20/02/2017.
 */
public class Station implements Serializable,Comparable<Station> {
    Double nBike=0.0;
    Long id=0L;
    String name="";
    Double lt=0.0;
    Double lg=0.0;
    Double soleil=1.0;
    Long dtUpdate=System.currentTimeMillis();
    Integer minute=0;
    Integer day=0;
    Integer hour=0;
    Double nPlace=0.0; //0,1 ou 2

    /**
     *
     * @param jnode
     * @param temperature
     */
    public Station(JsonNode jnode, Double temperature) throws ParseException {
        this.id=jnode.get("number").asLong();
        this.name=jnode.get("name").asText();
        String sDate=jnode.get("last_update").asText();
        sDate=sDate.split("\\+")[0];
        Date dt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(sDate);
        this.dtUpdate=dt.getTime();

        this.nPlace=jnode.get("available_bike_stands").asDouble();
        if(this.nPlace>9)this.nPlace=10.0;

        this.nBike=jnode.get("available_bikes").asDouble();
        if(this.nBike>9)this.nBike=10.0;


        this.lt= Double.valueOf(jnode.get("position").get(0).asDouble());
        this.lg= Double.valueOf(jnode.get("position").get(1).asDouble());

        if(temperature<15)soleil=0.0; else soleil=1.0;

        this.day=dt.getDay();
        this.minute=Math.round(dt.getMinutes()/5)*5;
        this.hour=dt.getHours();
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
        this.minute=Math.round(new Date(date).getMinutes()/5)*5;
        this.day=new Date(date).getDay();
        this.soleil=s.getSoleil();
        this.name=s.getName();
    }

    public Station(Station s, Integer day, Integer hour, Integer minute, Double soleil) {
        this.id=s.getId();
        this.hour=hour;
        this.day=day;
        this.minute=minute;
        this.soleil=soleil;
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

    public Integer getMinute() {
        return minute;
    }

    public Double getnBike() {
        return nBike;
    }

    public void setnBike(Double nBike) {
        if(nBike>9)nBike=10.0;
        this.nBike = nBike;
    }

    public void setMinute(Integer minute) {
        this.minute = minute;
    }

    public Long getDtUpdate() {
        return dtUpdate;
    }

    public void setDtUpdate(Long dtUpdate) {
        this.dtUpdate = dtUpdate;
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
        if(nPlace>9)nPlace=10.0;
        this.nPlace = nPlace;
    }


    public Vector toVector() {
        DenseVector v=new DenseVector(new double[]{this.id,this.day,this.hour,this.minute,this.soleil});
        return v;
    }

    public String[] colsName() {
        return new String[]{"id","day","hour","minute","soleil"};
    }

    public String toHTML(){
        String html="<h1>"+this.name+"</h1>";
        html+="Le "+this.getDay()+" At "+new SimpleDateFormat("dd/MM HH:mm").format(this.dtUpdate)+" ("+this.getHour()+":"+this.getMinute()+") : ";
        html+=this.getnPlace()+" places & "+this.getnBike()+" bikes";
        return html;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Station station = (Station) o;

        if (nBike != null ? !nBike.equals(station.nBike) : station.nBike != null) return false;
        if (id != null ? !id.equals(station.id) : station.id != null) return false;
        if (soleil != null ? !soleil.equals(station.soleil) : station.soleil != null) return false;
        if (minute != null ? !minute.equals(station.minute) : station.minute != null) return false;
        if (day != null ? !day.equals(station.day) : station.day != null) return false;
        if (hour != null ? !hour.equals(station.hour) : station.hour != null) return false;
        return !(nPlace != null ? !nPlace.equals(station.nPlace) : station.nPlace != null);

    }

    @Override
    public int hashCode() {
        int result = nBike != null ? nBike.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (soleil != null ? soleil.hashCode() : 0);
        result = 31 * result + (minute != null ? minute.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        result = 31 * result + (hour != null ? hour.hashCode() : 0);
        result = 31 * result + (nPlace != null ? nPlace.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(Station station) {
        if(this.getId()>station.getId())return 1;
        if(this.getId()<station.getId())return -1;
        if(this.dtUpdate>station.dtUpdate)return 1;
        if(this.dtUpdate<station.dtUpdate)return -1;
        return 0;
    }
}
