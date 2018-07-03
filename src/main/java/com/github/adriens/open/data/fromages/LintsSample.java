/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.adriens.open.data.fromages;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import tech.tablesaw.api.Table;
import tech.tablesaw.reducing.CrossTab;

/**
 *
 * @author salad74
 */
public class LintsSample {

    public static void main(String[] args) {
        try{
            Table table = Table.read().csv("lints.csv");
            System.out.println(table.shape());
            System.out.println(table.structure());
            // drop the "message" column
            table.removeColumns("message");
            System.out.println(table.first(5));
            
            table.removeColumns("objectName");
            table.removeColumns("value");
            System.out.println(table.first(5));
            
            //
            Class.forName("org.h2.Driver");
            //Connection conn = DriverManager.getConnection("jdbc:h2:mem:lints", "sa", "");
            Connection conn = DriverManager.getConnection("jdbc:h2:~/lints", "sa", "");
            Statement stmt = conn.createStatement();
            stmt.execute("drop table lints if exists");
            stmt.execute("CREATE TABLE LINTS AS SELECT * FROM CSVREAD('lints.csv')");
            
            ResultSet rs = stmt.executeQuery("select * from lints limit 10");
             while (rs.next()) {
                 System.out.println(rs.getString("linterId"));
             }
            
            
            stmt.executeQuery("CALL CSVWRITE('~/grouped-lints.csv', 'select severity, count(*) from lints group by severity')");
            System.exit(0);
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
