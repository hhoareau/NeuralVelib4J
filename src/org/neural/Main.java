package org.neural;

import spark.Spark;
import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class Main {
    private static final String NOW_FILE = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json";
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
    private static MySpark spark=null;
    private static Datas stations=new Datas();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException {

        Integer port=9999;
        if(args.length>0)port= Integer.valueOf(args[0]);
        logger.info("Ouverture du port "+port);
        Spark.port(port);

        File d=new File("./files");
        if(!d.exists())d.mkdir();

        logger.info("Creation d'un certificat pour la navigation https");
        Tools.createCertificate();

        logger.info("Lancement de l'environnement spark");
        spark=new MySpark("Java Spark SQL basic example");

        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    if(horaire.endsWith("0") || horaire.endsWith("5")){
                        stations.add(new Datas(NOW_FILE));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);


        final Runnable trainRefresh= new Runnable() {
            public void run() {
                try {
                    spark.train(stations,500);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,100, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,100,TimeUnit.MINUTES);


        //test : http://localhost:9999/use/PARADIS/0/0
        Spark.get("/use/:station/:delay/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)html="Aucune station";
            Iterator<Station> ite=stations.getIterator();
            while(ite.hasNext()){
                Station s=ite.next();
                if(s.getName().indexOf(request.params("station").toUpperCase())>0){
                    Long date=System.currentTimeMillis()+Long.valueOf(request.params("delay"))*1000*60;
                    Station station=new Station(s,date,Double.valueOf(request.params("soleil")));
                    station.nPlace=spark.predict(station.toVector().toDense());
                    html+=station.toHTML();
                }
            }
            return html;
        });


        Spark.get("/evaluate", (request, response) -> {
            return spark.evaluate(new Datas(NOW_FILE));
        });


        Spark.get("/load", (request, response) -> {
            stations.add(new Datas(NOW_FILE));
            return stations.toHTML(2000);
        });

        Spark.get("/loadall", (request, response) -> {
            stations=new Datas(1.0);
            return stations.toHTML(2000);
        });

        Spark.get("/list", (request, response) -> {
            String html="Files :<br>";
            for(File f:new File("./files/").listFiles())
                if(f.getName().startsWith("velib"))
                    html+=f.getName()+"<br>";

            return html;
        });

        Spark.get("/stations", (request, response) -> {
            return stations.toHTML(2000);
        });

        Spark.get("/train/:iter", (request, response) -> {
            if(stations.getSize()==0)stations=new Datas(NOW_FILE);
            spark.train(stations, Integer.valueOf(request.params("iter")));
            return spark.evaluate(new Datas(NOW_FILE));
        });

        Spark.get("/weights", (request, response) -> {
            return spark.showWeights();
        });

        Spark.get("/raz", (request, response) -> {
            File f=new File("./files/velib.w");
            f.delete();
            return "ok";
        });

        Spark.get("/razstations", (request, response) -> {
            stations=new Datas();
            return "ok";
        });


        Spark.get("/", (request, response) -> {
            String html="commands : <br>";
            html+="<a href='./help'>Help</a><br>";

            html+="<h2>"+stations.getSize()+" stations</h2>";
            html+="<a href='./load'>Add stations for now</a><br>";
            html+="<a href='./loadall'>Add all stations</a><br>";
            html+="<a href='./stations'>Stations list</a><br>";
            html+="<a href='./list'>List Files</a><br>";
            html+="<a href='./razstations'>Raz stations</a><br>";

            html+="<h2>Train</h2>";
            html+="<a href='./train/100'>Train on all</a><br>";
            html+="<a href='./evaluate'>Evaluate</a><br>";
            html+="<a href='./use/paradis/0/0'>Use</a><br>";
            html+="<a href='./weights'>Weights</a><br>";
            html+="<a href='./raz'>Raz</a><br>";

            return html;
        });
    }


}
