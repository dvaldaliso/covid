/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.refineria.bd.covidtema3;

import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author desarrollo
 */
public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Base de datos covid").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataFrame = spark.read().option("header", true).option("inferSchema", "true").csv("covid_autopesquisa.csv");
        //Iniciso 1
        dataFrame.printSchema();
        dataFrame.show(10);

        //3 - Numero de fila y columnas del dataframe
        Long filas = dataFrame.count();
        int columnas = dataFrame.columns().length;
        System.out.println("cantidad fila: " + filas + " Cantidad de columnas: " + columnas);

        Dataset<Row> logregdataall = dataFrame.select(col("Nombre"),
                col("Edad"), col("Sexo"),
                col("Policlinico"));

        //2 - Eliminar valores ausentes
        Dataset<Row> logredata = logregdataall.na().drop();

        //4 - Muestra las 50 primeras personas ordenadas descendentemente por la edad
        logredata.orderBy(col("Edad").desc()).show(50);

        //5 - Personas que han sido contactos con positivos
        logredata.filter(col("Contacto_con_positivo").equalTo("Si")).show();

        //6 - Personas que han sido contactos con positivos
        Long cantPersonasConContacto = logredata.filter(col("Contacto_con_positivo").equalTo("Si")).count();
        System.out.println("cantidad de personas que tuvieron contactos con postivos: " + cantPersonasConContacto);

        //7- Personas con al menos una efnermedad cronica       
        //logredata.filter((FilterFunction<Row>) r -> r.getAs("") == "Si" || r.getString(12) == "Si").show();
        dataFrame.createOrReplaceTempView("pesquisa");
        Dataset<Row> conEnfermedad = spark.sql("select Nombre, Edad, Sexo, Policlinico FROM pesquisa where Diabetes = 'Si' OR Cancer = 'Si' OR Insuficiencia_Cardiaca = 'Si' OR VIH = 'Si' OR Cancer = 'Si' OR Hipertension = 'Si' OR Enfermedad_Coronaria = 'Si'  ");
        conEnfermedad.show();
        //8 - Promedio de Edad de personas con diabetes y sexo femenino
        dataFrame.filter(col("Diabetes").equalTo("Si")).filter(col("Sexo").equalTo("F")).agg(functions.avg(col("Edad"))).show();
        //9
        Dataset<Row> sintoma = spark.sql("SELECT * FROM pesquisa where Fiebre = 'Si' or Dolor_de_Garganta = 'Si' or Dolor_de_Cabeza = 'Si' or Tos = 'Si' or Falta_de_Aire = 'Si'");
        sintoma.show();
        //10 Por sexo mueste cuantas personas han viajado
        logredata.filter(col("Han_Viajado").equalTo("Si")).groupBy(col("Sexo")).count().show();
        spark.stop();
    }

}
