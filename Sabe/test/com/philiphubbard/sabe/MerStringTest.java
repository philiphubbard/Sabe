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

// Confidence tests for the MerString class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class MerStringTest {

	public static void test() {
		System.out.println("Testing MerString:");
		
		String s1 = "A";
		int m1 = Mer.toInt(s1);
		MerString ms1 = new MerString(m1, 1);
		assert(ms1.toStringFinal().equals(s1));
		String s1_2 = ms1.toString();
		MerString ms1_2 = new MerString(s1_2);
		assert(ms1_2.toStringFinal().equals(s1));
		
		String s2 = "CG";
		int m2 = Mer.toInt(s2);
		MerString ms2 = new MerString(m2, 2);
		assert(ms2.toStringFinal().equals(s2));
		String s2_2 = ms2.toString();
		MerString ms2_2 = new MerString(s2_2);
		assert(ms2_2.toStringFinal().equals(s2));
		
		String s3 = "GTA";
		int m3 = Mer.toInt(s3);
		MerString ms3 = new MerString(m3, 3);
		assert(ms3.toStringFinal().equals(s3));
		String s3_2 = ms3.toString();
		MerString ms3_2 = new MerString(s3_2);
		assert(ms3_2.toStringFinal().equals(s3));
		
		String s4 = "TACG";
		int m4 = Mer.toInt(s4);
		MerString ms4 = new MerString(m4, 4);
		assert(ms4.toStringFinal().equals(s4));
		String s4_2 = ms4.toString();
		MerString ms4_2 = new MerString(s4_2);
		assert (ms4_2.toStringFinal().equals(s4));
		
		String s5 = "CGTAC";
		int m5 = Mer.toInt(s5);
		MerString ms5 = new MerString(m5, 5);
		assert(ms5.toStringFinal().equals(s5));
		String s5_2 = ms5.toString();
		MerString ms5_2 = new MerString(s5_2);
		assert (ms5_2.toStringFinal().equals(s5));
		
		String s6 = "ACGTAC";
		int m6 = Mer.toInt(s6);
		MerString ms6 = new MerString(m6, 6);
		assert(ms6.toStringFinal().equals(s6));
		String s6_2 = ms6.toString();
		MerString ms6_2 = new MerString(s6_2);
		assert (ms6_2.toStringFinal().equals(s6));
		
		ms2.merge(ms3_2, 1);
		assert(ms2.toStringFinal().equals("CGTA"));
		
		ms3.merge(ms4, 2);
		assert(ms3.toStringFinal().equals("GTACG"));
		
		ms4.merge(ms5, 2);
		assert(ms4.toStringFinal().equals("TACGTAC"));
		
		ms5.merge(ms6, 2);
		assert(ms5.toStringFinal().equals("CGTACGTAC"));
		
		ms5.merge(ms4, 3);
		assert(ms5.toStringFinal().equals("CGTACGTACGTAC"));
		
		ms5.merge(ms5, 9);
		assert(ms5.toStringFinal().equals("CGTACGTACGTACGTAC"));
		
		System.out.println("MerString passed");
	}
}
