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
import java.util.logging.FileHandler;
import java.util.logging.Logger;


public class Main {
    private static final String NOW_FILE = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json";
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
    private static FileHandler logFile=null;
    private static MySpark spark=null;
    private static Datas stations=null;
    private static String filter="PARADIS";
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException, ParseException {

        logger.addHandler(new FileHandler("./files/logfile.txt"));

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
        stations=new Datas(spark);

        //Récupération du fichier
        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    if(horaire.endsWith("0") || horaire.endsWith("5")){
                        logger.info("Récuperation d'un fichier velib");
                        stations.add(Tools.getStations(Tools.getData(null),1.0,null),spark.getSession());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,10,TimeUnit.MINUTES);


        final Runnable trainRefresh= new Runnable() {
            public void run() {
                try {
                    logger.info("New train task");
                    if(stations.getSize()==0)stations=new Datas(spark.getSession(),"./files",null);
                    spark.train(stations,100);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,5, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(trainRefresh,3,TimeUnit.MINUTES);


        Spark.get("/use/:station/:day/:hour/:minute/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)stations=new Datas(1.0,filter);

            Station s=stations.getStation(request.params("station"));
            if(s!=null){
                Station station=new Station(s,Integer.valueOf(request.params("day")), Integer.valueOf(request.params("hour")),Integer.valueOf(request.params("minute")),Double.valueOf(request.params("soleil")));
                html+="Input : "+station.toVector().toString();
                html+=Tools.DatasetToHTML(spark.predict(station));
                return html;
            }
            return html;
        });


        //test : http://localhost:9999/use/PARADIS/0/0
        Spark.get("/use/:station/:delay/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)stations=new Datas(1.0,filter);

            Station s=stations.getStation(request.params("station").toUpperCase());
            if(s!=null){
                    Long date=System.currentTimeMillis()+Long.valueOf(request.params("delay"))*1000*60;
                    Station station=new Station(s,date,Double.valueOf(request.params("soleil")));
                    html+="Input : "+station.toVector().toString()+"<br><br>";
                    html+=Tools.DatasetToHTML(spark.predict(station));
                    return html;
            }
            return html;
        });


        Spark.get("/evaluate", (request, response) -> {
            return spark.evaluate(stations);
        });


        Spark.get("/load", (request, response) -> {
            Map<String,Double> data=Tools.getMeteo(new Date(System.currentTimeMillis()));
            stations.add(Tools.getStations(Tools.getData(NOW_FILE,"./files/velib_"+System.currentTimeMillis()+".json"),data.get("temperature"),filter),spark.getSession());
            return stations.toHTML(2000);
        });

        Spark.get("/loadall", (request, response) -> {
            filter=null;
            //stations=new Datas(1.0,null);
            stations=new Datas(spark.getSession(),"./files",null);
            stations.save();
            if(stations.getSize()<2000)
                return stations.toHTML(2000);
            else
                return "ok";
        });

        Spark.get("/loadwithfilter/:filter", (request, response) -> {
            filter=request.params("filter");
            stations=new Datas(spark.getSession(),"./files",filter);
            return stations.toHTML(2000);
        });

        Spark.get("/persist", (request, response) -> {
            stations.createTrain().persist();
            return "save";
        });


        Spark.get("/show", (request, response) -> {
            return stations.showData();
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
            if(stations.getSize()==0)return "load stations before train";
            return spark.train(stations, Integer.valueOf(request.params("iter")));
        });

        Spark.get("/weights", (request, response) -> {
            return spark.showWeights();
        });

        Spark.get("/log", (request, response) -> {
            BufferedReader f = new BufferedReader(new InputStreamReader(new FileInputStream("./files/logfile.txt")));
            String rc="";
            String line=f.readLine();
            while(line!=null){
                rc+=line;
                line=f.readLine();
            }
            f.close();
            return rc;

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

            html+="<h2>"+stations.getSize()+" stations / filter="+filter+"</h2>";
            html+="<a href='./load'>Add stations for now</a><br>";
            html+="<a href='./loadall'>Add all stations</a><br>";
            html+="<a href='./loadwithfilter/10019%20-%20PARADIS'>Add stations with filter "+filter+" </a><br>";
            html+="<a href='./stations'>Stations list</a><br>";
            html+="<a href='./list'>List Files</a><br>";
            html+="<a href='./show'>Show inputs</a><br>";
            html+="<a href='./razstations'>Raz stations</a><br>";

            html+="<h2>Train</h2>";
            html+="<a href='./train/10'>Train on all with 10</a><br>";
            html+="<a href='./evaluate'>Evaluate</a><br>";
            html+="<a href='./use/10019%20-%20PARADIS/0/0'>Use</a><br>";
            html+="<a href='./use/10019%20-%20PARADIS/6/18/25/0'>Use avec station/day/hour/minute/soleil</a><br>";
            html+="<a href='./weights'>Weights</a><br>";
            html+="<a href='./raz'>Raz</a><br>";

            html+="<h2>Tools</h2>";
            html+="<a href='localhost:4040'>Admin</a><br>";
            html+="<a href='./log'>Log</a><br>";


            return html;
        });
    }


}
