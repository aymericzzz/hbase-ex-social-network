import java.io.IOException;
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

    private static Configuration conf = null;
    static{
        conf = HBaseConfiguration.create();
    }

    public static boolean isAlpha(String name) {
        char[] chars = name.toCharArray();

        for (char c : chars) {
            if(!Character.isLetter(c) && !Character.isDigit(c)) {
                return true;
            }
        }
        return false;
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

    public static void main(String[] args) throws Exception {
        boolean c = true;
        String choice = "";
        String firstName;
        String value;
        Scanner sc = new Scanner(System.in);
        String tableName = "azhuoBFF";
        String[] columnFamilies = { "info", "friends" };

        HBase.createTable(tableName, columnFamilies);
        System.out.println("Welcome to the BFF Social Network!");
        do {
            System.out.println("Enter your BFFID (remixed first name) : ");
            firstName = sc.nextLine();
            while (isAlpha(firstName)) {
                System.out.println("Your BFFID must be uniquely composed of letters and digits. Try again : ");
                firstName = sc.nextLine();
            }

            System.out.println("Enter your age : ");
            value = sc.nextLine();
            while (!value.matches("\\d+")) {
                System.out.println("An age is composed of digits dumbass. Try again : ");
                value = sc.nextLine();
            }
            HBase.addRecord(tableName, firstName, "info", "age", value);

            System.out.println("Enter your gender (M or F) : ");
            value = sc.nextLine();
            while (!value.equals("M") && !value.equals("F")) {
                System.out.println("M or F, ffs. Try again : ");
                value = sc.nextLine();
            }
            HBase.addRecord(tableName, firstName, "info", "gender", value);

            HBase.getAllRecord(tableName);

            System.out.println("Do you want to add a new person? y or n");
            choice = sc.nextLine();
            while(choice.charAt(0) != 'n' && choice.charAt(0) != 'y')
            {
                System.out.println(choice + " " + choice.charAt(0));
                System.out.println("YES = y AND NO = n. Now try again : ");
                choice = "";
                choice = sc.nextLine();
            }
            if(choice.charAt(0) == 'n')
                c = false;
        }while(c);
        System.out.println("See you again ~~");
    }
}
