/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package introDBacesso;

import java.sql.*;
import java.util.ArrayList;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileNotFoundException;

/*--------Trabalho apresentado á disciplina de Introdução à Bando de Dados--------*/
/*--Realizar conexão com bando de dados MySQL através da api JDBC linguagem Java--*/
/*Consultar todas as tabelas do banco de dados e exportar arquivos em formato .csv*/
/*Conjunto de caracteres Unicode(UTF-8), idioma padrão-ingles(EUA), delimitador de*/
/*------------texto aspas duplas ("texto"), separado por vírgula (,)--------------*/
/*--------Consultar tabelas específicas, caso sejam passadas por parâmetro--------*/
/*---A chamada do prigrama deve conter no mínimo quatro parâmestros sendo eles:---*/
/*-1.Servidor 2.Banco de Dados 3.Usuário 4. Senha seguidos de parâmetros contendo-*/
/*-----------------nomes das tabelas (de maneira opcional)------------------------*/
/*Ao executar o projeto no NETBEANS devem ser definidos os patâmetros de entrada--*/
/* Project->Properties->Run->Arguments-------------------ou de maneira opcional em*/
/* Run->Set Project Configuration->Customize->Arguments---------------------------*/
/*--------------------------- @author saulobergamo--------------------------------*/

public class Consulta1 {
        
    public static void main(String[] args) throws Exception {        
        //carregar o driver do MySQL
        Class.forName("com.mysql.cj.jdbc.Driver");      
        //iniciar conexão: recebendo servidor, banco de dados, usuário e senha na chamadado programa(linha de comando))        
        Connection con = DriverManager.getConnection("jdbc:mysql://" + args[0] + "/" + args[1], args[2], args[3]);         
        //criar statement        
        Statement stmt = con.createStatement();
        //criar ResultSet;
        ResultSet rs;// = null;
                
        if(args.length > 4){    
            //repete enquanto não chegar ao final de args.length que são os parâmetros passados na chamada do programa
            for(int i = 4; i < args.length; i++){                
                //chamar query para acessar e exportar cada tabela   
                rs = stmt.executeQuery("select * from "+args[i]);
                ResultSetMetaData rsmd = rs.getMetaData();                                
                //define o nome do arquivo csv a ser criado //Ex.: university-parâmetro.csv
                //Cria arquivos no mesmo diretório de "Consulta1.java"
                try(PrintWriter wrt = new PrintWriter(new File("src/introDBacesso/"+args[1]+"-"+args[i]+".csv"))){
                    StringBuilder sb = new StringBuilder();
                        //percorre as colunas da tabela, exporta o cabeçalho com nome das colunas
                        for(int j = 1; j <= rsmd.getColumnCount(); j++){
                            sb.append("\"");
                            sb.append(rsmd.getColumnName(j));
                            sb.append("\"");
                            sb.append(", ");
                           
                        }
                        sb.delete(sb.length()-1, sb.length());
                        wrt.write(sb.toString());
                        sb.delete(0, sb.length());
		      	sb.append('\n');
                        //percorre a tabela e seu conteúdo e exporta o conteúdo de toda a tabela
                        while(rs.next()){
                            for(int j = 1; j <= rsmd.getColumnCount(); j++){
                                sb.append("\"");
                                sb.append(rs.getString(j));
                                sb.append("\"");
                                sb.append(", ");                                
                              
                            }
                            sb.delete(sb.length()-1, sb.length());
                            wrt.write(sb.toString());
                            sb.delete(0, sb.length());
                            sb.append('\n');
                        }
                }
                catch (FileNotFoundException e) {
                    System.out.println(e.getMessage());
                }                           
            }                                                                                   
        }
        
        else{
            //se foram apenas os parâmetros relativos à servidor, banco de dados, usuário e senha, então é necessário exportar todas as tabelas           
            //executar query que mostra tabelas do banco de dados
            rs = stmt.executeQuery("show tables" );
            //percorre a única coluna da tabela "show tables" contendo as tabelas disponíveis no banco de dados e guarda no array 'tabelas' cada nome
            ArrayList<String> tabelas = new ArrayList<>();
            while(rs.next()){
                tabelas.add(rs.getString(1));
            }
            //repete enquanto não encerrar todas as tabelas do banco de dados consulta os nomes no array "tabelas"
            for(int i = 0; i < tabelas.size(); i++){                          
                //chamar query para acessar e exportar todo o conteúdo de cada tabela   
                rs = stmt.executeQuery("select * from "+tabelas.get(i));
                ResultSetMetaData rsmd = rs.getMetaData();
                //Cria arquivos no mesmo diretório de "Consulta1.java"
                try(PrintWriter wrt = new PrintWriter(new File("src/introDBacesso/"+args[1]+"-"+tabelas.get(i)+".csv"))){
                    StringBuilder sb = new StringBuilder();
                        //percorre as colunas da tabela, exporta o cabeçalho com nome das colunas                
                        for(int j = 1; j <= rsmd.getColumnCount(); j++){
                            sb.append("\"");
                            sb.append(rsmd.getColumnName(j));
                            sb.append("\"");
                            sb.append(", ");                            
                        }
                        sb.delete(sb.length()-1, sb.length());
                        wrt.write(sb.toString());
                        sb.delete(0, sb.length());
                        sb.append("\n");
                        //percorre a tabela e seu conteúdo e exporta o conteúdo de toda a tabela
                        while(rs.next()){
                            for(int j = 1; j <= rsmd.getColumnCount(); j++){
                                sb.append("\"");
                                sb.append(rs.getString(j));
                                sb.append("\"");
                                sb.append(", ");                                                                
                            }
                            sb.delete(sb.length()-1, sb.length());
                            wrt.write(sb.toString());
                            sb.delete(0, sb.length());
                            sb.append("\n");
                        }
                }
                catch (FileNotFoundException e) {
                    System.out.println(e.getMessage());
                }
            }                                               
            //exporta o arquivo com nome Ex.: university-students.csv args[1]+'-'+tabelas.get[i].csv
            //repete enquanto não chegar ao final das tabelas
        }
        con.close();
    }            
}