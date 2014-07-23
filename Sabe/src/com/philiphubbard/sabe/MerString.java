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

// A class to support the merging of Mer instances.
// A MerString can be built from a Mer instance, and then it can be
// merged with another MerString given an overlap region.
// For example, if MS1 is the MerString from "ACGT" and MS2 is the
// MerString from "CGTA" then merging MS1 with MS2 for an overlap of 3
// produces the MerString MS3 for "ACGTA".  MS3 then could be merged
// with other MerStrings in similar manner.
// Since a Mer can contain only the characters "A", "C", "G", "T",
// a MerString can store each character with just 2 bits.  (Internally,
// the characters are known as "letters" to distinguish them from the
// char data type.)

public class MerString {

	public MerString(int mer, int length) {
		int byteLength = length / LETTERS_PER_BYTE;
		if (length % LETTERS_PER_BYTE > 0)
			byteLength++;
		byteLength += HEADER_LENGTH;
		bytes = new byte[byteLength];
		
		String merString = Mer.fromInt(mer, length);
		for (int i = 0; i < length; i++){
			int letter;
			switch (merString.charAt(i)) {
			case 'A':
				letter = A;
				break;
			case 'C':
				letter = C;
				break;
			case 'G':
				letter = G;
				break;
			default:
				letter = T;
			}
			put(bytes, i, letter);
		}
				
		this.length = length;
		putHeader(bytes);
	}
	
	// HEY!!
	public MerString(String s) {
		getHeader(s);
		string = s;
	}
	
	public MerString(byte[] bytes) {
		this.bytes = bytes;
		getHeader(this.bytes);
	}
	
	public MerString(byte[] bytes, int i, int n) {
		this.bytes = new byte[n];
		for (int j = 0; j < n; j++)
			this.bytes[j] = bytes[j + i];
		getHeader(this.bytes);
	}
	
	// Merges "other" into this MerString.  Does not verify that the letters
	// in the overlap region are actually the same.
	
	public void merge(MerString other, int overlap) {
		int mergedLength = length + other.length - overlap;
		
		int byteLength = mergedLength / LETTERS_PER_BYTE;
		if (mergedLength % LETTERS_PER_BYTE > 0)
			byteLength++;
		byteLength += HEADER_LENGTH;
		byte[] result = new byte[byteLength];
		
		for (int i = 0; i < bytes.length; i++)
			result[i] = bytes[i];
		
		int i = length;
		for (int j = overlap; j < other.length; j++) {
			int letter = get(other.bytes, j);
			put(result, i++, letter);
		}

		length = mergedLength;
		putHeader(result);		
		bytes = result;
	}
	
	// HEY!!
	// Outputs the encoded form of the data as a String, which is not
	// intendend to be read by a human.
	
	public String toString() {
		return string;
	}
	
	public byte[] toBytes() {
		return bytes;
	}
	
	// Produces a final, human-readable String for the MerString.
	
	public String toDisplayString() {
		char[] array = new char[length];
		
		for (int i = 0; i < length; i++) {
			int letter = get(bytes, i);
			switch (letter) {
			case A:
				array[i] = 'A';
				break;
			case C:
				array[i] = 'C';
				break;
			case G:
				array[i] = 'G';
				break;
			default:
				array[i] = 'T';
			}
		}

		return new String(array);
	}
	
	public boolean equals(MerString other) {
		if (bytes.length != other.bytes.length)
			return false;
		for (int i = 0; i < bytes.length; i++)
			if (bytes[i] != other.bytes[i])
				return false;
		return true;
	}
	
	//
	
	private void putHeader(char[] array) {
		int extra = length % LETTERS_PER_CHAR;
		array[0] = (char) extra;
	}
	
	private void getHeader(String string) {
		int extra = (int) string.charAt(0);
		int lengthInChars = string.length() - HEADER_LENGTH;
		if (extra == 0)
			length = lengthInChars * LETTERS_PER_CHAR;
		else
			length = (lengthInChars - 1) * LETTERS_PER_CHAR + extra;
	}
	
	private void put(char[] array, int i, int letter) {
		int iChar = i / LETTERS_PER_CHAR;
		int r = i % LETTERS_PER_CHAR;
		letter <<= BITS_PER_LETTER * (LETTERS_PER_CHAR - 1 - r);
		array[iChar + HEADER_LENGTH] |= letter;
	}
	
	private int get(String string, int i) {
		int iChar = i / LETTERS_PER_CHAR;
		int r = i % LETTERS_PER_CHAR;
		int letter = (int) string.charAt(HEADER_LENGTH + iChar);
		letter >>>= BITS_PER_LETTER * (LETTERS_PER_CHAR - 1 - r);
		return (int) (letter & LETTER_BIT_MASK);
	}
	
	private void putHeader(byte[] array) {
		int extra = length % LETTERS_PER_BYTE;
		array[0] = (byte) extra;
	}
	
	private void getHeader(byte[] array) {
		int extra = (int) array[0];
		int lengthInBytes = bytes.length - HEADER_LENGTH;
		if (extra == 0)
			length = lengthInBytes * LETTERS_PER_BYTE;
		else
			length = (lengthInBytes - 1) * LETTERS_PER_BYTE + extra;
	}
	
	private static void put(byte[] array, int i, int letter) {
		int iByte = i / LETTERS_PER_BYTE;
		int r = i % LETTERS_PER_BYTE;
		letter <<= BITS_PER_LETTER * (LETTERS_PER_BYTE - 1 - r);
		array[iByte + HEADER_LENGTH] |= letter;
	}
	
	private static int get(byte[] array, int i) {
		int iByte = i / LETTERS_PER_BYTE;
		int r = i % LETTERS_PER_BYTE;
		int letter = (int) array[HEADER_LENGTH + iByte];
		letter >>>= BITS_PER_LETTER * (LETTERS_PER_BYTE - 1 - r);
		return (int) (letter & LETTER_BIT_MASK);
	}
	
	private static final int BITS_PER_LETTER = 2;
	private static final int LETTER_BIT_MASK = 0x3;
	
	// HEY!!
	private static final int LETTERS_PER_CHAR = 8;

	private static final int LETTERS_PER_BYTE = 4;
	private static final int A = 0x0;
	private static final int C = 0x1;
	private static final int G = 0x2;
	private static final int T = 0x3;
	private static final int HEADER_LENGTH = 1;
	
	private int length;
	private byte[] bytes;
	
	// HEY!!
	private String string;
}
