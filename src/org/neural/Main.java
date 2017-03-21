package org.neural;

import spark.Spark;

import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;


public class Main {
    private static final String NOW_FILE = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json";
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
    private static FileHandler logFile=null;
    private static MySpark spark=new MySpark("Java Spark SQL basic example",32);;
    private static Datas stations=new Datas(spark.getSession());
    private static String filter="PA";
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);




    public static int[] getLayer(String sLayer){
        String[] sLayers=sLayer.split("-");
        int[] layer=new int[sLayers.length];
        for(int k=0;k<layer.length;k++)layer[k]= Integer.parseInt(sLayers[k]);
        return layer;
    }

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException, ParseException {
        System.setProperty("hadoop.home.dir", "c:/temp");

        logger.addHandler(new FileHandler("./files/logfile.txt"));

        //SparkDl4jMultiLayer sparkNet = new SparkDl4jMultiLayer(sc, conf, tm);

        Integer port=9999;
        if(args.length>0)port= Integer.valueOf(args[0]);
        logger.info("Ouverture du port "+port);
        Spark.port(port);

        File d=new File("./files");
        if(!d.exists())d.mkdir();

        logger.info("Creation d'un certificat pour la navigation https");
        Tools.createCertificate();

        logger.info("Lancement de l'environnement spark");
        //stations=new Datas(spark.getSession(),"./files",filter);

        //Récupération du fichier
        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    if(horaire.endsWith("0") || horaire.endsWith("5")){
                        logger.info("Récuperation d'un fichier velib");
                        stations.add(Tools.getStations(Tools.getData("./files/velib_"+System.currentTimeMillis()+".json"),1.0,null),spark.getSession());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(commandRefresh,1, TimeUnit.MINUTES);

        final Runnable trainRefresh= new Runnable() {
            public void run() {
                try {
                    logger.info("New train task");
                    if(stations.getSize()==0 || spark.onTrain())return;
                    spark.train(stations,500,(int[]) null);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,10, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(trainRefresh,10,TimeUnit.MINUTES);


        Spark.get("/use/:layer/:station/:day/:hour/:minute/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)stations=new Datas(1.0,filter);

            Station s=stations.getStation(request.params("station"));
            if(s!=null){
                Station station=new Station(s,Integer.valueOf(request.params("day")), Integer.valueOf(request.params("hour")),Integer.valueOf(request.params("minute")),Double.valueOf(request.params("soleil")));
                html+="Input : "+station.toVector().toString();
                html+=Tools.DatasetToHTML(spark.predict(new Datas(spark.getSession(),s),getLayer(request.params("layer"))));
                return html;
            }
            return html;
        });


        //test : http://localhost:9999/use/PARADIS/0/0
        Spark.get("/use/:layer/:station/:delay/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)stations=new Datas(1.0,filter);

            Station s=stations.getStation(request.params("station").toUpperCase());
            if(s!=null){
                    Long date=System.currentTimeMillis()+Long.valueOf(request.params("delay"))*1000*60;
                    Station station=new Station(s,date,Double.valueOf(request.params("soleil")));
                    html+="Input : "+station.toVector().toString()+"<br><br>";
                    html+=Tools.DatasetToHTML(spark.predict(new Datas(spark.getSession(),station),getLayer(request.params("layer"))));
                    return html;
            }
            return html;
        });



        Spark.get("/evaluate/:layer", (request, response) -> {
            return spark.evaluate(stations);
        });


        Spark.get("/load", (request, response) -> {
            Map<String,Double> data=Tools.getMeteo(new Date(System.currentTimeMillis()));
            stations.add(Tools.getStations(Tools.getData(NOW_FILE,"./files/velib_"+System.currentTimeMillis()+".json"),data.get("temperature"),filter),spark.getSession());
            return stations.toHTML(2000);
        });

        Spark.get("/loadall", (request, response) -> {
            filter=null;
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
            return stations.showData(1000);
        });


        Spark.get("/toCSV/:layer", (request, response) -> {
            response.type("text/csv");
            return stations.toCSV(spark,"\t","\r\n",getLayer(request.params("layer")));
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


        Spark.get("/search/:iter", (request, response) -> {
            if(stations.getSize()==0)return "load stations before train";
            Integer nIter=Integer.valueOf(request.params("iter"));

            int inputLayer=new Station().colsName().length;
            int nOut=9;

            Collection<int[]> layers= new HashSet<>();
            layers.add(new int[]{inputLayer,20,20,nOut});
            layers.add(new int[]{inputLayer,50,50,nOut});
            layers.add(new int[]{inputLayer,100,100,nOut});
            layers.add(new int[]{inputLayer,50,50,50,nOut});
            layers.add(new int[]{inputLayer,10,10,10,10,nOut});
            layers.add(new int[]{inputLayer,20,20,20,20,nOut});
            layers.add(new int[]{inputLayer,60,nOut});
            layers.add(new int[]{inputLayer,500,nOut});


            String rc=spark.findModel(stations, nIter,layers);
            rc+="Meileur model : "+spark.showLayers()+"<br>";

            return rc;
        });

        Spark.get("/train/:iter/:layers", (request, response) -> {
            if(stations.getSize()==0)return "load stations before train";
            if(spark.onTrain())return "Only one train";

            Integer nIter=Integer.valueOf(request.params("iter"));


            Integer decoupe=1;
            if(nIter>10000)decoupe=5;


            int step=nIter/decoupe;
            String rc="";
            for(int i=nIter;i>=0;i=i-step){
                rc+=spark.train(stations, step,getLayer(request.params("layers")));
                logger.warning(rc);
            }

            return rc;

        });


        Spark.get("/train/:iter", (request, response) -> {
            if(stations.getSize()==0)return "load stations before train";

            Integer nIter=Integer.valueOf(request.params("iter"));
            if(nIter<6)
                return spark.train(stations, nIter, (int[]) null);
            else{
                int step=nIter/5;
                String rc="";
                for(int i=nIter;i>=0;i=i-step){
                    rc+=spark.train(stations, step, (int[]) null);
                    logger.warning(rc);
                }
                return rc;
            }
        });



        Spark.get("/weights", (request, response) -> {
            return spark.showWeights(null);
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


        Spark.get("/size", (request, response) -> {
            return stations.getSize();
        });


        Spark.get("/raz", (request, response) -> {
            File f=new File("./files/velib.w");
            f.delete();
            return "ok";
        });

        Spark.get("/razstations", (request, response) -> {
            stations=new Datas(spark.getSession());
            return "ok";
        });

        Spark.get("/", (request, response) -> {
            String html="commands : <br>";
            html+="<a href='./help'>Help</a><br>";

            html+="<script>function httpGet(service,func){" +
                    "var xhr = new XMLHttpRequest();xhr.open('GET', service, true);" +
                    "xhr.onreadystatechange = function(e) {if (xhr.readyState == 4)func(xhr.responseText);};" +
                    "xhr.send();" +
                    "};</script>";

            html+="<script>var modele='5-60-9';var filter='PA';" +
                    "function changeFilter(){filter=prompt(\"change filter\");show();}" +
                    "function show(){httpGet('./size',function(s){document.getElementById('size').innerHTML='size:'+s});document.getElementById('modele').innerHTML='layers:'+modele;document.getElementById('titre').innerHTML='filter:'+filter;}" +
                    "function changeModel(){modele=prompt(\"layers\");show();}" +
                    "function loadStation(){window.open('../loadwithfilter/'+filter);}" +
                    "function findModel(){iter=prompt('iterations');window.open('./search/'+iter);}" +
                    "function train(){iter=prompt('iterations');window.open('../train/'+iter+'/'+modele);}" +
                    "function evaluate(){window.open('../evaluate/'+modele);}" +
                    "function toCSV(){window.open('./toCSV/'+modele);}" +
                    "</script>";


            html+="<h2><div  style='display:inline' id='titre'></div>&nbsp;<div  style='display:inline' id='size'></div></h2>";
            html+="<a href='javascript:changeFilter()'>Change filter</a><br>";
            html+="<a href='./load'>Add stations for now</a><br>";
            html+="<a href='./loadall'>Add all stations</a><br>";
            html+="<a href='javascript:loadStation()'>Add stations with filter</a><br>";

            html+="<h2>Show</h2>";
            html+="<a href='./stations'>Stations list</a><br>";
            html+="<a href='./list'>List Files</a><br>";
            html+="<a href='./show'>Show inputs</a><br>";
            html+="<a href='./razstations'>Raz stations</a><br>";

            html+="<h2>Modele:<div style='display:inline' id='modele'></div></h2>";
            html+="<a href='javascript:findModel()'>Search model</a><br>";
            html+="<a href='javascript:train()'>Train model</a><br>";
            html+="<a href='javascript:evaluate()'>Evaluate modele</a><br>";
            html+="<a href='./use/10019%20-%20PARADIS/0/0'>Use</a><br>";
            html+="<a href='./use/10019%20-%20PARADIS/6/18/25/0'>Use avec station/day/hour/minute/soleil</a><br>";
            html+="<a href='./weights'>Weights</a><br>";
            html+="<a href='./raz'>Raz</a><br>";

            html+="<h2>Tools</h2>";
            html+="<a href='javascript:toCSV()'>CSV</a><br>";
            html+="<a href='localhost:4040'>Admin</a><br>";
            html+="<a href='./log'>Log</a><br>";

            html+="<script>show();</script>";

            return html;
        });
    }


}
