// Copyright (c) 2013 Philip M. Hubbard
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
		int charLength = length / LettersPerChar;
		if (length % LettersPerChar > 0)
			charLength++;
		charLength += HeaderLength;
		char[] array = new char[charLength];
		
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
			put(array, i, letter);
		}
				
		this.length = length;
		putHeader(array);
		this.string = new String(array);
	}
	
	public MerString(String s) {
		getHeader(s);
		this.string = s;
	}
	
	// Merges "other" into this MerString.  Does not verify that the letters
	// in the overlap region are actually the same.
	
	public void merge(MerString other, int overlap) {
		int mergedLength = this.length + other.length - overlap;
		
		int charLength = mergedLength / LettersPerChar;
		if (length % LettersPerChar > 0)
			charLength++;
		charLength += HeaderLength;
		char[] array = new char[charLength];
		
		for (int i = 0; i < this.string.length(); i++)
			array[i] = this.string.charAt(i);
		
		int i = this.length;
		for (int j = overlap; j < other.length; j++) {
			int letter = get(other.string, j);
			put(array, i++, letter);
		}

		this.length = mergedLength;
		putHeader(array);		
		this.string = new String(array);
	}
	
	// Outputs the encoded form of the data as a String, which is not
	// intendend to be read by a human.
	
	public String toString() {
		return this.string;
	}
	
	// Produces a final, human-readable String for the MerString.
	
	public String toStringFinal() {
		char[] array = new char[this.length];
		
		for (int i = 0; i < this.length; i++) {
			int letter = get(this.string, i);
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
	
	//
	
	private void putHeader(char[] array) {
		int extra = this.length % LettersPerChar;
		array[0] = (char) extra;
	}
	
	private void getHeader(String string) {
		int extra = (int) string.charAt(0);
		int lengthInChars = string.length() - HeaderLength;
		if (lengthInChars % LettersPerChar == 0)
			this.length = LettersPerChar * lengthInChars;
		else
			this.length = LettersPerChar * (lengthInChars - 1) + extra;
	}
	
	private void put(char[] array, int i, int letter) {
		int iChar = i / LettersPerChar;
		int r = i % LettersPerChar;
		letter <<= BitsPerLetter * (LettersPerChar - 1 - r);
		array[iChar + HeaderLength] |= letter;
	}
	
	private int get(String string, int i) {
		int iChar = i / LettersPerChar;
		int r = i % LettersPerChar;
		int letter = (int) string.charAt(HeaderLength + iChar);
		letter >>>= BitsPerLetter * (LettersPerChar - 1 - r); // HEY!! (14 - 2 * r);
		return (int) (letter & LetterBitMask);
	}
	
	private static final int BitsPerLetter = 2;
	private static final int LetterBitMask = 0x3;
	private static final int LettersPerChar = 8;
	private static final int A = 0x0;
	private static final int C = 0x1;
	private static final int G = 0x2;
	private static final int T = 0x3;
	private static final int HeaderLength = 1;
	
	private int length;
	private String string;
}
