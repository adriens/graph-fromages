/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.adriens.open.data.fromages;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvReadOptions;

/**
 *
 * @author salad74
 */
public class GexfBuilder {
    
    private Connection conn;

    public static void main(String[] args) {
        GexfBuilder builder = new GexfBuilder();
        try {
            //builder.getOpenCsv();
            builder.setUpDatabase();
            builder.createGexfFile();
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public GexfBuilder() {

    }

    public void setUpDatabase() throws Exception{
        Class.forName("org.h2.Driver");
            //Connection conn = DriverManager.getConnection("jdbc:h2:mem:lints", "sa", "");
            //this.conn = DriverManager.getConnection("jdbc:h2:~/fromages", "sa", "");
            this.conn = DriverManager.getConnection("jdbc:h2:mem:fomrages", "sa", "");
            Statement stmt = conn.createStatement();
            stmt.execute("drop table fromages if exists");
            stmt.execute("CREATE TABLE fromages AS SELECT * FROM CSVREAD('fromages.csv',NULL,'charset=UTF-8 fieldSeparator=;');");
            stmt.execute("create index idx_dep on fromages(departement)");
            stmt.execute("create index idx_lait on fromages(lait)");
            stmt.execute("create index idx_fromage on fromages(fromage)");
            stmt.execute("update fromages set lait='Vache' where lait='Vaches'");
            
            ResultSet rs = stmt.executeQuery("select * from fromages limit 10");
            // some cleanup
            
            
            
             while (rs.next()) {
                 System.out.println(rs.getString("departement"));
             }
             
    }
    
    public void createGexfFile() throws Exception{
        //FileWriter fw = new FileWriter("fromages.gexf");
	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("fromages.gexf"), StandardCharsets.UTF_8));
        bw.write("<?xml version=\"1.1\" encoding=\"UTF-8\"?>");
        bw.write("<gexf xmlns=\"http://www.gexf.net/1.3draft\"\n" +
"       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
"       xsi:schemaLocation=\"http://www.gexf.net/1.3draft\n" +
"                             http://www.gexf.net/1.3draft/gexf.xsd\"\n" +
"      version=\"1.3\">");
        //bw.write("");
        bw.write("<graph defaultedgetype=\"directed\">\n");
        
        // put nodes
        bw.write("<nodes>\n");
        Statement stmt = conn.createStatement();
        ResultSet rs = null;
        
        // Put department nodes       
        /*
        rs = stmt.executeQuery("select distinct departement from fromages order by departement");
        while (rs.next()) {
                 bw.write("<node id=\"" + rs.getString("departement") + "\" label=\"" + rs.getString("departement") + "\"/>\n");
         }
        */
        
        bw.flush();
        // put fromages list
        rs = stmt.executeQuery("select distinct fromage from fromages order by fromage");
        while (rs.next()) {
                 bw.write("<node id=\"" + rs.getString("fromage") + "\" label=\"" + rs.getString("fromage") + "\"/>\n");
         }
        bw.flush();
        
        // laits
        rs = stmt.executeQuery("select distinct lait from fromages where lait not like '%,%' order by lait");
        while (rs.next()) {
                 bw.write("<node id=\"" + rs.getString("lait") + "\" label=\"" + rs.getString("lait") + "\"/>\n");
        }
        bw.flush();
        
        bw.write("</nodes>\n");
        bw.write("<edges>\n");
        
        int i = 0;
        
        
        
        //link fromage to region
        /*
        rs = stmt.executeQuery("select fromage, departement from fromages order by fromage");
        while (rs.next()) {
                 //bw.write("<node id=\"" + rs.getString("lait") + "\" label=\"" + rs.getString("lait") + "\"/>\n");
                 bw.write("<edge id=\"" + i + "\" source=\"" + rs.getString("fromage") + "\" target=\"" + rs.getString("departement") + "\"/>\n");
                 i++;
        }
        bw.flush();
        */
        
        //link fromage to region
        rs = stmt.executeQuery("select distinct fromage, lait from fromages where lait not like '%,%' order by fromage");
        
        while (rs.next()) {
                 //bw.write("<node id=\"" + rs.getString("lait") + "\" label=\"" + rs.getString("lait") + "\"/>\n");
                 bw.write("<edge id=\"" + i + "\" source=\"" + rs.getString("fromage") + "\" target=\"" + rs.getString("lait") + "\"/>\n");
                 i++;
        }
        bw.flush();
        
        rs = stmt.executeQuery("select fromage, lait from fromages where lait like '%,%' order by fromage");
        
        while (rs.next()) {
                 //bw.write("<node id=\"" + rs.getString("lait") + "\" label=\"" + rs.getString("lait") + "\"/>\n");
                 bw.write("<edge id=\"" + i + "\" source=\"" + rs.getString("fromage") + "\" target=\"Brebis\"/>\n");
                 i++;
                 bw.write("<edge id=\"" + i + "\" source=\"" + rs.getString("fromage") + "\" target=\"Chèvre\"/>\n");
                 i++;
                 bw.write("<edge id=\"" + i + "\" source=\"" + rs.getString("fromage") + "\" target=\"Vache\"/>\n");
                 i++;
        }
        bw.flush();
        
        bw.write("");
        bw.write("");
        
        bw.write("</edges>");
        bw.write("</graph>\n" +
"</gexf>");
        
        
        bw.flush();
        bw.close();
        
    }
    public void importDataToTable() throws IOException {
        //ColumnType[] types = {};
        System.out.println("Loading csv into tablesaw...");
        String location = "https://public.opendatasoft.com//api/records/1.0/download?dataset=fromagescsv-fromagescsv";
        Table table;
        InputStream input = new URL(location).openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
        table = Table.read().csv(CsvReadOptions.builder(reader, "fromages").separator(';'));

        System.out.println("Table loaded.");
        System.out.println("nb cols : " + table.name());
        System.out.println("nb cols : " + table.column(0).unique().print());
        
        Column regions = table.column(0).unique();
        regions.sortAscending();
        System.out.println(regions.getString(0));
        System.out.println(regions.getString(1));
        int nbRegions = regions.size();
        int i = 0;
        while (i < nbRegions){
            System.out.println(regions.getString(i));
            i++;
        }
        // now, fetch distiinct laits
        Column laits = null;
        laits = table.column(5).unique();
        System.out.println("Nb laits : " + laits.size());
        laits.print();
        int nbLaits = laits.size();
        i = 0;
        while (i < nbLaits){
            System.out.println(laits.getString(i));
            i++;
        }
        
        // now fetch fromages to build links...
        System.out.println("now fetch fromages to build links...");
        Column fromagesList = table.column(1);
        fromagesList.unique();
        fromagesList.sortAscending();
        System.out.println("Nb fromages : " + fromagesList.size());
        fromagesList = table.column(1).unique();
        int nbFromages = fromagesList.size();
        i = 0;
        while (i < nbFromages){
            System.out.println(fromagesList.getString(i));
            i++;
        }
        
        
        

    }

    public void buildGexfFile() throws IOException {
        // first, put the nodes

        Table fromagesTable = Table.read().csv("fomages.csv");
    }

    public void getOpenCsv() throws IOException {
        System.out.println("Getting the fromages csv from the web...");
        String url = "https://public.opendatasoft.com//api/records/1.0/download?dataset=fromagescsv-fromagescsv";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        FileWriter writer = new FileWriter(new File("fromages.csv"));
        BufferedWriter bw = new BufferedWriter(writer);

        //StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            //result.append(line);
            bw.write(line + "\n");
        }
        bw.flush();
        bw.close();
        System.out.println("Successfullt got the csv.");
    }
}
