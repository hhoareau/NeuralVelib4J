package org.neural;

import org.codehaus.jackson.JsonNode;
import spark.ModelAndView;
import spark.Spark;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

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

import static spark.Spark.before;
import static spark.Spark.options;


public class Main {
    private static final String NOW_FILE = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download?format=json";
    private static Logger logger = Logger.getLogger(String.valueOf(Main.class));
    private static FileHandler logFile=null;
    private static MySpark spark=new MySpark("Java Spark SQL basic example",32);;
    private static Datas stations=new Datas(spark.getSession());
    private static String filter="PA";

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static Map map=new HashMap<>();

    // Enables CORS on requests. This method is an initialization method and should be called once.
    private static void enableCORS(final String origin, final String methods, final String headers) {
        options("/*", (request, response) -> {

            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }

            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if (accessControlRequestMethod != null) {
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }

            return "OK";
        });

        before((request, response) -> {
            response.header("Access-Control-Allow-Origin", origin);
            response.header("Access-Control-Request-Method", methods);
            response.header("Access-Control-Allow-Headers", headers);
            // Note: this may or may not be necessary in your particular application
            response.type("application/json");
        });
    }

    public static void updMap(){
        map.put("filter",filter);
        map.put("size",stations.getSize());
        map.put("onTraining",spark.onTrain());
        map.put("modele",spark.getCurrentModele());
    }

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException, ParseException {
        System.setProperty("hadoop.home.dir", "c:/temp");
        logger.addHandler(new FileHandler("./files/logfile.txt"));

        Integer port=9999;
        if(args.length>0)port= Integer.valueOf(args[0]);
        Spark.port(port);
        logger.warning("Ouverture du port "+port);

        File d=new File("./files");
        if(!d.exists()){
            logger.warning("Création du répertoire de récupération des fichiers");
            d.mkdir();
        }

        logger.warning("Creation d'un certificat pour la navigation https");
        Tools.createCertificate();

        logger.warning("Lancement de l'environnement spark");
        //stations=new Datas(spark.getSession(),"./files",filter);

        //Récupération du fichier
        final Runnable commandRefresh = new Runnable() {
            public void run() {
                try {
                    String horaire=new SimpleDateFormat("hh:mm").format(new Date(System.currentTimeMillis()));
                    if(horaire.endsWith("0") || horaire.endsWith("5")){
                        logger.warning("Récuperation d'un fichier velib");
                        JsonNode result = Tools.getData("./files/velib_" + System.currentTimeMillis() + ".json");
                        if(result!=null)logger.warning("Transfert vers disque");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,1, TimeUnit.MINUTES);
            }
        };
        logger.warning("Installation de la tache de récupération des velib");
        scheduler.schedule(commandRefresh,1, TimeUnit.MINUTES);

        final Runnable trainRefresh= new Runnable() {
            public void run() {
                try {
                    logger.info("New train task");
                    if(stations.getSize()==0 || spark.onTrain())return;
                    spark.train(stations,500);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                scheduler.schedule(this,10, TimeUnit.MINUTES);
            }
        };
        scheduler.schedule(trainRefresh,10,TimeUnit.MINUTES);

        //enableCORS("*","get","");

        Spark.get("/usecsv", (request, response) -> {
            response.type("text/csv");
            return Tools.toCSV(spark.predict(stations),";","\n");
        });

        Spark.get("/use/:station/:day/:hour/:minute/:soleil", (request, response) -> {
            String html="";
            if(stations.getSize()==0)stations=new Datas(1.0,filter);
            Station s=stations.getStation(request.params("station"));
            if(s!=null){
                Station station=new Station(s,Integer.valueOf(request.params("day")), Integer.valueOf(request.params("hour")),Integer.valueOf(request.params("minute")),Double.valueOf(request.params("soleil")));
                html+="Input : "+station.toVector().toString();
                html+=Tools.DatasetToHTML(spark.predict(new Datas(spark.getSession(),s)).toString());
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
                    html+=Tools.DatasetToHTML(spark.predict(new Datas(spark.getSession(),station)).toString());
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
            return stations.getSize()+" stations loaded";
        });

        Spark.get("/loadwithfilter", (request, response) -> {
            stations=new Datas(spark.getSession(),"./files",filter);
            return stations.getSize();
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
            return stations.toCSV(spark,"\t","\r\n",Tools.asArray(request.params("layer")));
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
            layers.add(new int[]{inputLayer,300,300,nOut});
            layers.add(new int[]{inputLayer,50,50,50,nOut});
            layers.add(new int[]{inputLayer,10,10,10,10,nOut});
            layers.add(new int[]{inputLayer,20,20,20,20,nOut});
            layers.add(new int[]{inputLayer,60,nOut});
            layers.add(new int[]{inputLayer,500,nOut});
            layers.add(new int[]{inputLayer,5000,nOut});


            String rc=spark.findModel(stations, nIter,layers);
            updMap();

            return rc;
        });

        Spark.get("/setfilter/:filter", (request, response) -> {
            filter= request.params("filter");
            return "filter="+filter;
        });

        Spark.get("/setmodele/:layers", (request, response) -> {
            String layers = request.params("layers");
            spark.setModele(new Station().colsName().length+"-"+layers+"-9");
            return "Model "+layers+" loaded with weights";
        });

        Spark.get("/train/:iter", (request, response) -> {
            if(stations.getSize()==0)return "load stations before train";
            if(spark.onTrain())return "Only one train";
            if(spark.getCurrentModele()==null)return "You must set a model before";

            Integer nIter=Integer.valueOf(request.params("iter"));

            Integer decoupe=1;
            if(nIter>10000)decoupe=5;

            int step=nIter/decoupe;
            String rc="";
            for(int i=nIter;i>=0;i=i-step){
                rc+=spark.train(stations, step);
                logger.warning(rc);
            }

            return rc;
        });


        Spark.get("/train/:iter", (request, response) -> {
            if(stations.getSize()==0)return "load stations before train";

            Integer nIter=Integer.valueOf(request.params("iter"));
            if(nIter<6)
                return spark.train(stations, nIter);
            else{
                int step=nIter/5;
                String rc="";
                for(int i=nIter;i>=0;i=i-step){
                    rc+=spark.train(stations, step);
                    logger.warning(rc);
                }
                return rc;
            }
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


        Spark.get("/infos", (request, response) -> {
            return stations.getSize()+";"+spark.getCurrentModele()+";"+filter;
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

        Spark.get("/menu", (request, response) -> new ModelAndView(map,"Main"),new ThymeleafTemplateEngine());

        Spark.get("/showSpark", (request, response) -> new ModelAndView(map,"showSpark"),new ThymeleafTemplateEngine());
    }
}
