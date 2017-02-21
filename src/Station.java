import org.codehaus.jackson.JsonNode;
import scala.Serializable;
import java.util.Date;

/**
 * Created by Hervé on 20/02/2017.
 */
public class Station implements Serializable {
    Long id=0L;

    Double lt=0.0;
    Double lg=0.0;
    Double temperature=0.0;
    Double month=0.0;
    Double minute=0.0;
    Double day=0.0;
    Double hour=0.0;
    Double nPlace=0.0; //0,1 ou 2

    public Station(JsonNode jnode, Double temperature, Long dt){
        this.id=jnode.get("number").asLong();

        this.nPlace=jnode.get("available_bike_stands").asDouble();
        if(this.nPlace>0 && this.nPlace<10)this.nPlace=1.0;
        if(this.nPlace>=10)this.nPlace=2.0;

        this.lt= Double.valueOf(jnode.get("position").get(0).asDouble());
        this.lg= Double.valueOf(jnode.get("position").get(1).asDouble());

        this.temperature=temperature/30.0;
        this.day=new Date(dt).getDay()/7.0;
        this.minute=new Date(dt).getMinutes()/60.0;
        this.month=new Date(dt).getMonth()/12.0;
        this.hour=new Date(dt).getHours()/24.0;

        //this.features=new SparseVector(6,new int[]{0,1,2,3,4,5},new double[]{this.day,this.minute,this.month,this.day,this.temperature,this.id});
    }

    /*
    public org.apache.spark.sql.Row toRow(){
        return RowFactory.create(this.nPlace, Vectors.dense(id, this.lt,this.lg, this.temperature,0.1,this.day,this.month,this.hour,this.minute));
    }
    */

    //public SparseVector getFeatures() {return features;}
    //public void setFeatures(SparseVector features) {this.features = features;}


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

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getMonth() {
        return month;
    }

    public void setMonth(Double month) {
        this.month = month;
    }

    public Double getMinute() {
        return minute;
    }

    public void setMinute(Double minute) {
        this.minute = minute;
    }

    public Double getDay() {
        return day;
    }

    public void setDay(Double day) {
        this.day = day;
    }

    public Double getHour() {
        return hour;
    }

    public void setHour(Double hour) {
        this.hour = hour;
    }

    public Double getnPlace() {
        return nPlace;
    }

    public void setnPlace(Double nPlace) {
        this.nPlace = nPlace;
    }
}
