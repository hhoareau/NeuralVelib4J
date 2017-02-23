package org.neural;

import spark.Spark;
import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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
                        spark.train(stations,100);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,0,TimeUnit.MINUTES);



        //test : http://localhost:9999/use/PARADIS/0/0
        Spark.get("/use/:station/:delay/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)html="Aucune station";
            for(Station s:stations.getStations())
                if(s.getName().indexOf(request.params("station"))>0){
                    Long date=System.currentTimeMillis()+Long.valueOf(request.params("delay"))*1000*60;
                    Station station=new Station(s,date,Double.valueOf(request.params("soleil")));
                    station.nPlace=spark.predict(station.toVector().toDense());
                    html+=station.toHTML();
                }
            return html;
        });


        Spark.get("/evaluate", (request, response) -> {
            return spark.evaluate(new Datas(NOW_FILE));
        });


        Spark.get("/load", (request, response) -> {
            stations=new Datas(NOW_FILE);
            return stations.toHTML();
        });


        Spark.get("/list", (request, response) -> {
            String html="Files :<br>";
            for(File f:new File("./files/").listFiles())
                if(f.getName().startsWith("velib"))
                    html+=f.getName()+"<br>";

            return html;
        });

        Spark.get("/stations", (request, response) -> {
            return stations.toHTML();
        });

        Spark.get("/train/:iter", (request, response) -> {
            spark.train(new Datas(1.0), Integer.valueOf(request.params("iter")));
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

        Spark.get("/", (request, response) -> {
            String html="commands : <br>";
            html+="<a href='./help'>Help</a><br>";
            html+="<a href='./weights'>Weights</a><br>";
            html+="<a href='./evaluate'>Evaluate</a><br>";
            html+="<a href='./load'>Load stations</a><br>";
            html+="<a href='./list'>List Files</a><br>";
            html+="<a href='./stations'>Stations list</a><br>";
            html+="<a href='./train/100'>Train on all</a><br>";
            html+="<a href='./use/paradis/0/0'>Use</a><br>";
            html+="<a href='./raz'>Raz</a><br>";
            return html;
        });
    }


}
