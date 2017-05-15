"Neural Velib" est un serveur REST développé en Java dont l'objectif est
- d'effectuer un relevé toutes les 5 minutes de l'état de l'ensemble des stations velib sur Paris,
- d'entrainer un réseau de neurones (Perceptron Multi Couche) sur la base des relevés précédemment évoqué avec comme input : la station, la date et l'heure de relevé, la météo et comme output : le nombre de places et de velib à la station considéré
- d'utiliser le réseau de neurone précédemment entrainé pour prévoir l'état de la station à une date donnée

Il ne s'agit pas d'une application de production mais plutôt d'un exemple d'utilisation en Java du framework Spark et en particulier de sa librairie "Machine Learning" sur lequel il repose.

Le déploiement du serveur est simplifié par l'usage de Docker. Ainsi, il est déployable simplement par la commande :
 - "docker run --restart=always -d -p 9999:9999 -p 9998:4040 -v /home/neural-velib:/home/files hhoareau/neural-velib" pour une distribution Linux
- "docker run --restart=always -d -p 9999:9999 -p 9998:4040 -v /home/neural-velib:/home/files hhoareau/neural-velib-pc" pour un PC

L'accès au serveur se fait simplement par http://localhost:9999/menu
