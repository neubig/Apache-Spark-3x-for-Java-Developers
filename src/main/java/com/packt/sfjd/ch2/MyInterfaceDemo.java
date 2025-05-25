package com.packt.sfjd.ch2;

public class MyInterfaceDemo {
    public static void main(String[] args) {
        System.out.println("Testing MyInterface implementation:");
        MyInterfaceImpl obj = new MyInterfaceImpl();
        System.out.println("Default method result: " + obj.hello());
        System.out.println("Calling abstract method:");
        obj.absmethod();
    }
}
