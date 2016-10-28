1. Set le pom.xml avec les d�pendances hbase, ainsi que les propri�t�s dans <plugins> et <build>. Trouvable � cette adresse : http://www.informit.com/articles/article.aspx?p=2255108&seqNum=2
2. �crire son code Java qui cr�e par exemple une table HBase
3. r�cup�rer � partir du cluster le fichier hbase-site.xml � partir de la commande scp : 
scp id@CLUSTER_IP_ADD:/etc/hbase/conf/hbase-site.xml LOCAL_DESTINATION_DIRECTORY
4. mettre ce fichier dans votre r�pertoire de projet dans un dossier conf. Vous devriez avoir TP_BFF/conf/hbase-site.xml, TP_BFF �tant le nom de votre projet.

I. Pour run � partir du cluster:
1. sur intellij, cr�er une configuration Maven et pr�ciser dans command line "clean install"
2. Lancer le build avec la configuration cr��e
3. si votre projet a build sans probl�me, faire une commande scp (comme pour le tp mapreduce) pour envoyer le .jar cr�� (dans target) sur le cluster
4. ex�cuter le .jar avec une commande du type "hadoop jar nom_de_mon_jar.jar nom_de_ma_classe_principale"

II. Pour run � partir de intellij:
1. sur intellij, cr�er une configuration Application, dans laquelle vous sp�cifierez dans "Main Class", votre classe principale
2. Plus bas dans la fen�tre, appuyez sur le "+" vert pour ajouter le param�tre "Run Maven Goal", et dans la fen�tre pop-up, pr�ciser "clean install" dans command line.
3. Cliquer sur Apply et OK
4. Lancer le projet avec cette configuration. Le code devrait interagir avec le cluster sans que vous ayez � vous connecter dessus. Ajouter des println pour vous assurer que les actions � effectuer ont �t� ex�cut�es (ex: table toto created!)