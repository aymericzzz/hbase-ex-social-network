import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Aymeric on 21/10/2016.
 */

public class HBase {

    // initialisation de la variable configuration
    private static Configuration conf = null;
    // instanciation
    static{
        conf = HBaseConfiguration.create();
    }

    // fonction pour vérifier si une string est alphanumérique
    public static boolean isAlpha(String name) {
        // convertit la string en char array
        char[] chars = name.toCharArray();
        // vérifie que chaque char est soit une lettre, soit un digit
        for (char c : chars) {
            if(!Character.isLetter(c) && !Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] families)
            throws Exception {
        // nouvelle instance hbaseadmin
        HBaseAdmin admin = new HBaseAdmin(conf);
        // si la table à créer existe déjà, on ne fait rien
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            // sinon on la crée, en instanciant d'abord un HTableDescriptor avec la tablename en paramètre
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            // et on itère sur familys pour obtenir les columns familys à créer
            for (int i = 0; i < families.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(families[i]));
            }
            // création de la table
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            // initialisation du scanner
            Scan s = new Scan();
            // lecture du scanner
            ResultScanner ss = table.getScanner(s);
            // pour chaque résultat
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    // print "row CF:property ts value"
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * method that retrieves every single rowid in the table
     * @param tableName
     * @return an arrayList with the results
     */
    public static ArrayList<String> getNames (String tableName) {
        ArrayList<String> res = new ArrayList<String>();
        String temp = new String();

        try{
            HTable table = new HTable(conf, tableName);
            // initialisation du scanner
            Scan s = new Scan();
            // lecture du scanner
            ResultScanner ss = table.getScanner(s);
            // pour chaque résultat
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    // si l'arraylyst ne contient pas déjà la rowid, on l'ajoute
                    if(!res.contains(new String(kv.getRow())))
                        res.add(new String(kv.getRow()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return res;
    }

    /**
     * Put (or insert) a row
     */

    // nom de la table, rowkey (friendID), column family(info), property(age), value(42)
    // cette fonction permet d'ajouter par exemple à thomas/info:age -> 42
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            // obtention de l'instance htable
            HTable table = new HTable(conf, tableName);
            // création de la rowKey
            Put put = new Put(Bytes.toBytes(rowKey));
            // ajout de la rowkey avec les infos en paramètre de la fonction
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recorded " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * main
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // initialisation des ressources
        ArrayList<String> namesList = new ArrayList<String>();
        ArrayList<String> friendsArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean c = true;
        boolean d = true;
        String choice = "";
        String firstName;
        String value;
        Scanner sc = new Scanner(System.in);
        String tableName = "azhuoBFF";
        String[] columnFamilies = {"info", "friends"};

        // création de la table HBase
        HBase.createTable(tableName, columnFamilies);
        // récupération de toutes les rowid différentes et stockage dans namesList
        namesList = getNames(tableName);
        System.out.println("People in the database : ");
        System.out.println(Arrays.toString(namesList.toArray()));
        System.out.println("Welcome to the BFF Social Network!");

        // boucle d'ajout de profil
        do {
            // saisie de l'ID
            System.out.println("Enter your BFFID (remixed first name) : ");
            firstName = sc.nextLine();
            // tant que l'ID saisi n'est pas alphanumérique, on recommence la saisie
            while (!isAlpha(firstName)) {
                System.out.println("Your BFFID must be uniquely composed of letters and digits. Try again : ");
                firstName = sc.nextLine();
            }
            // tant que l'ID saisi existe dans la DB, on recommence
            while(namesList.contains(firstName))
            {
                System.out.println("Sorry, this BFFID has been taken, choose another one, pal : ");
                firstName = sc.nextLine();
                while (!isAlpha(firstName)) {
                    System.out.println("Your BFFID must be uniquely composed of letters and digits. Try again : ");
                    firstName = sc.nextLine();
                }
            }

            // entrée de l'âge
            System.out.println("Enter your age : ");
            value = sc.nextLine();
            // tant que la saisie n'est pas un nombre (regex)
            while (!value.matches("\\d+")) {
                System.out.println("An age is composed of digits dumbass. Try again : ");
                value = sc.nextLine();
            }
            // ajout de l'entrée dans la base dans info:age
            HBase.addRecord(tableName, firstName, "info", "age", value);

            // saisie du gender
            System.out.println("Enter your gender (M or F) : ");
            value = sc.nextLine();
            // tant que la saisie est différente de M ou F
            while (!value.equals("M") && !value.equals("F")) {
                System.out.println("M or F, ffs. Try again : ");
                value = sc.nextLine();
            }
            // ajout de l'entrée dans info/gender
            HBase.addRecord(tableName, firstName, "info", "gender", value);

            // saisie du BFF
            System.out.println("Who is your BFF alias BEST FRIEND FOREVER? ");
            // si la DB est déjà populée
            if(!namesList.isEmpty()) {
                System.out.println("Here are the persons you can choose : ");
                System.out.println(Arrays.toString(namesList.toArray()));
                value = sc.nextLine();
                while (!isAlpha(value)) {
                    System.out.println("HOW COULD YOU MISPELL THE NAME OF YOUR BFF?! Try again : ");
                    value = sc.nextLine();
                }
                // tant que la saisie n'existe pas dans la DB (puisque le BFF doit bien exister => choix de programmation)
                while(!namesList.contains(value)) {
                    System.out.println("Well, your BFF doesn't exist in our database, choose another one : ");
                    value = sc.nextLine();
                }
            }
            // si la DB est vide, aucun BFF n'est présent, on laisse le user ajouter qui il veut comme BFF
            else{
                System.out.println("Well, the DB is empty, feel free to add the BFF you want : ");
                while (!isAlpha(value)) {
                    System.out.println("HOW COULD YOU MISPELL THE NAME OF YOUR BFF?! Try again : ");
                    value = sc.nextLine();
                }
            }
            // ajout dans friends:BFF
            HBase.addRecord(tableName, firstName, "friends", "BFF", value);

            // demande si le user veut rajouter d'autres amis
            do {
                System.out.println("Do you wish to add a (useless) friend in the \"others\" section? y or n");
                choice = sc.nextLine();
                while (choice.charAt(0) != 'n' && choice.charAt(0) != 'y') {
                    System.out.println("YES = y AND NO = n. Now try again : ");
                    choice = sc.nextLine();
                }
                // si choix == n, on sort de la boucle
                if(choice.charAt(0) == 'n') {
                    d = false;
                    break;
                }

                // saisie de l'autre ami
                System.out.println("What is the name of this useless friend?");
                value = sc.nextLine();
                // vérifie qu'il existe dans la base
                while (!namesList.contains(value)) {
                    System.out.println("I know (s)he is useless, but (s)he doesn't exist neither. Try again : ");
                    value = sc.nextLine();
                }
                // on l'ajoute dans une arraylist
                friendsArray.add(value);
            }while(d);
            // on construit une string (qu'on va stocker dans la DB) à partir de l'arraylist de string
            for(String s : friendsArray)
            {
                sb.append(s);
                sb.append(", ");
            }
            // ajout de l'entrée dans friends:others
            HBase.addRecord(tableName, firstName, "friends", "others", sb.toString());
            // affichage de toutes les entrées de la base
            HBase.getAllRecord(tableName);

            // demande si le user veut ajouter un autre profil
            System.out.println("Do you want to add a new person (create another BFFID)? y or n");
            choice = sc.nextLine();
            while(choice.charAt(0) != 'n' && choice.charAt(0) != 'y')
            {
                System.out.println("YES = y AND NO = n. Now, try again : ");
                choice = sc.nextLine();
            }
            // si choice == 'n', sortie de boucle
            if(choice.charAt(0) == 'n')
                c = false;
        }while(c);
        System.out.println("See you again ~~");
    }
}
