package org.example;

public class Logit {

    public static void log(String s){
        System.out.println(s);
    }

    public static void error(Throwable t){
        t.printStackTrace();
    }

    public static void error(String s){
        System.out.println("ERROR: " + s);
    }

}
