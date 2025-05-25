package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.List;

public class ShortCircuitOperationExample {

	public static void main(String[] args) {
		// Create a list of member names
		List<String> memberNames = Arrays.asList("Adam", "Bob", "Charles", "David", "Lisa", "Lucy");
		
		// Check if any name starts with "A"
		boolean matched = memberNames.stream()
                .anyMatch((s) -> s.startsWith("A"));

		System.out.println("Any name starts with 'A': " + matched);

		// Find the first name that starts with "L"
		String firstMatchedName = memberNames.stream()
			.filter((s) -> s.startsWith("L"))
			.findFirst().get();

		System.out.println("First name starting with 'L': " + firstMatchedName);
		
		// Check if all names have length > 3
		boolean allLongerThan3 = memberNames.stream()
			.allMatch(s -> s.length() > 3);
		
		System.out.println("All names longer than 3 characters: " + allLongerThan3);
		
		// Check if no name starts with "Z"
		boolean noNameStartsWithZ = memberNames.stream()
			.noneMatch(s -> s.startsWith("Z"));
		
		System.out.println("No name starts with 'Z': " + noNameStartsWithZ);
	}
}
