package com.packt.sfjd.ch2;

import java.io.File;

public class MyFilterImpl {
	public static void main(String[] args) {
		File dir = new File("/workspace/Apache-Spark-3x-for-Java-Developers/src/main/java/com/packt/sfjd/ch2");
		System.out.println("Directory exists: " + dir.exists());
		System.out.println("Directory is directory: " + dir.isDirectory());
		
		// Using MyFileNameFilter
		System.out.println("\nUsing MyFileNameFilter:");
		String[] filesWithFilter = dir.list(new MyFileNameFilter());
		if (filesWithFilter != null) {
			System.out.println("Found " + filesWithFilter.length + " files");
			for (String file : filesWithFilter) {
				System.out.println(file);
			}
		}
		
		// Using lambda
		System.out.println("\nUsing lambda:");
		String[] filesWithLambda = dir.list((dirname,name)->name.endsWith(".java"));
		if (filesWithLambda != null) {
			System.out.println("Found " + filesWithLambda.length + " files");
			for (String file : filesWithLambda) {
				System.out.println(file);
			}
		} else {
			System.out.println("Directory not found or is not a directory");
		}
	}
}
