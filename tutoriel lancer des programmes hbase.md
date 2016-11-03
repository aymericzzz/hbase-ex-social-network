1. Set le pom.xml avec les dépendances hbase, ainsi que les propriétés dans plugins et build. Trouvable à cette adresse : http://www.informit.com/articles/article.aspx?p=2255108&seqNum=2
2. écrire son code Java qui crée par exemple une table HBase
3. récupérer à partir du cluster le fichier hbase-site.xml à partir de la commande scp : 
scp id@CLUSTER_IP_ADD:/etc/hbase/conf/hbase-site.xml LOCAL_DESTINATION_DIRECTORY
4. mettre ce fichier dans votre répertoire de projet dans un dossier conf. Vous devriez avoir TP_BFF/conf/hbase-site.xml, TP_BFF étant le nom de votre projet.

I. Pour run à partir du cluster:
1. sur intellij, créer une configuration Maven et préciser dans command line "clean install"
2. Lancer le build avec la configuration créée
3. si votre projet a build sans problème, faire une commande scp (comme pour le tp mapreduce) pour envoyer le .jar créé (dans target) sur le cluster
4. exécuter le .jar avec une commande du type "hadoop jar nom_de_mon_jar.jar nom_de_ma_classe_principale"

II. Pour run à partir de intellij:
1. sur intellij, créer une configuration Application, dans laquelle vous spécifierez dans "Main Class", votre classe principale
2. Plus bas dans la fenêtre, appuyez sur le "+" vert pour ajouter le paramètre "Run Maven Goal", et dans la fenêtre pop-up, préciser "clean install" dans command line.
3. Cliquer sur Apply et OK
4. Lancer le projet avec cette configuration. Le code devrait interagir avec le cluster sans que vous ayez à vous connecter dessus. Ajouter des println pour vous assurer que les actions à effectuer ont été exécutées (ex: table toto created!)
