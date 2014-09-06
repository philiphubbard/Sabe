// Copyright (c) 2014 Philip M. Hubbard
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
// http://opensource.org/licenses/MIT

package com.philiphubbard.sabe;

// Utility routines for converting a k-mer string into a numerical index
// and vice-versa.  Since a k-mer can contain only the characters "A", "C",
// "G" and "T", the mapping involves using two bits for each character.

public class Mer {
	
	// Returns the numerical index from the specified k-mer.
	// Throws IllegalArgumentException if the mer is longer than what can be encoded
	// in a 32-bit signed int, or if the mer contains characters other than "A", "C", 
	// "G", "T" (or the lower-case equivalents).
	
	public static int toInt(String mer) throws IllegalArgumentException {
		if (mer.length() > 15)
			throw new IllegalArgumentException("Mer.toInt(): mer is too long");
		
		mer.toUpperCase();
		int result = 0;
		for (int i = 0; i < mer.length(); i++) {
			result <<= 2;
			// 'A' = 0x00
			if (mer.charAt(i) == 'C')
				result |= 0x1;
			else if (mer.charAt(i) == 'G')
				result |= 0x2;
			else if (mer.charAt(i) == 'T')
				result |= 0x3;
			else if (mer.charAt(i) != 'A')
				throw new IllegalArgumentException("Mer.toInt(): k-mer contains illegal character \'" 
						+ mer.charAt(i) + "\' [" + (int) mer.charAt(i) + "]");
		}

		return result;
	}
	
	// Returns the right-most characters of string k-mer from the specified numerical index.
	// The length argument specifies how many characters to return.
	// Throws IllegalArgumentException if the length is longer than what can be encoded
	// in a 32-bit signed int.
	
	public static String fromInt(int x, int length) throws IllegalArgumentException {
		if (length > 15)
			throw new IllegalArgumentException("Mer.fromInt(): requested length is too long");
		if (length <= 0)
			throw new IllegalArgumentException("Mer.fromInt(): length must be positive");

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < length; i++) {
			if ((x & 0x3) == 0x1)
				builder.insert(0, 'C');
			else if ((x & 0x3) == 0x2)
				builder.insert(0, 'G');
			else if ((x & 0x3) == 0x3)
				builder.insert(0, 'T');
			else
				builder.insert(0, 'A');
			x >>= 2;
		}
		return builder.toString();
	}
	
}
