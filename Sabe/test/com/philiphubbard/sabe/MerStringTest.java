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
		assert (ms1.toDisplayString().equals(s1));
		String s1a = ms1.toString();
		MerString ms1a = new MerString(s1a);
		assert (ms1a.toDisplayString().equals(s1));
		
		String s2 = "CG";
		int m2 = Mer.toInt(s2);
		MerString ms2 = new MerString(m2, 2);
		assert (ms2.toDisplayString().equals(s2));
		String s2a = ms2.toString();
		MerString ms2a = new MerString(s2a);
		assert (ms2a.toDisplayString().equals(s2));
		
		String s3 = "GTA";
		int m3 = Mer.toInt(s3);
		MerString ms3 = new MerString(m3, 3);
		assert (ms3.toDisplayString().equals(s3));
		String s3a = ms3.toString();
		MerString ms3a = new MerString(s3a);
		assert (ms3a.toDisplayString().equals(s3));
		
		String s4 = "TACG";
		int m4 = Mer.toInt(s4);
		MerString ms4 = new MerString(m4, 4);
		assert (ms4.toDisplayString().equals(s4));
		String s4a = ms4.toString();
		MerString ms4a = new MerString(s4a);
		assert (ms4a.toDisplayString().equals(s4));
		
		String s5 = "CGTAC";
		int m5 = Mer.toInt(s5);
		MerString ms5 = new MerString(m5, 5);
		assert (ms5.toDisplayString().equals(s5));
		String s5a = ms5.toString();
		MerString ms5a = new MerString(s5a);
		assert (ms5a.toDisplayString().equals(s5));
		
		String s6 = "ACGTAC";
		int m6 = Mer.toInt(s6);
		MerString ms6 = new MerString(m6, 6);
		assert (ms6.toDisplayString().equals(s6));
		String s6a = ms6.toString();
		MerString ms6a = new MerString(s6a);
		assert (ms6a.toDisplayString().equals(s6));
		
		ms2.merge(ms3, 1);
		String s2_3 = "CGTA";
		assert (ms2.toDisplayString().equals(s2_3));
		
		String s2b = ms2.toString();
		MerString ms2b = new MerString(s2b);
		assert (ms2b.toDisplayString().equals(s2_3));
		
		ms3.merge(ms4, 2);
		assert (ms3.toDisplayString().equals("GTACG"));
		
		ms4.merge(ms5, 2);
		assert (ms4.toDisplayString().equals("TACGTAC"));
		
		ms5.merge(ms6, 2);
		assert (ms5.toDisplayString().equals("CGTACGTAC"));
		
		ms5.merge(ms4, 3);
		assert (ms5.toDisplayString().equals("CGTACGTACGTAC"));
		
		ms5.merge(ms5, 9);
		assert (ms5.toDisplayString().equals("CGTACGTACGTACGTAC"));
		
		//
		
		String s7 = "ACGTAC";
		int m7 = Mer.toInt(s7);
		MerString ms7 = new MerString(m7, s7.length());
		
		String s8 = s7;
		int m8 = Mer.toInt(s8);
		MerString ms8 = new MerString(m8, s8.length());
		
		String s9 = "ACGTTC";
		int m9 = Mer.toInt(s9);
		MerString ms9 = new MerString(m9, s9.length());
		
		assert (ms7.equals(ms8));
		assert (!ms7.equals(ms9));
		assert (!ms8.equals(ms9));
		assert (ms9.equals(ms9));
		
		//
		
		String s10 = "ACGTACGT";
		int m10 = Mer.toInt(s10);
		MerString ms10 = new MerString(m10, 8);
		assert (ms10.toDisplayString().equals(s10));
		String s10a = ms10.toString();
		MerString ms10a = new MerString(s10a);
		assert (ms10a.toDisplayString().equals(s10));
		
		String s11 = "ACGTACGTTGCA";
		int m11 = Mer.toInt(s11);
		MerString ms11 = new MerString(m11, 12);
		assert (ms11.toDisplayString().equals(s11));
		String s11a = ms11.toString();
		MerString ms11a = new MerString(s11a);
		assert (ms11a.toDisplayString().equals(s11));
				
		String s12 = "ACGTACGTTGCATGC";
		int m12 = Mer.toInt(s12);
		MerString ms12 = new MerString(m12, 15);
		assert (ms12.toDisplayString().equals(s12));
		String s12a = ms12.toString();
		MerString ms12a = new MerString(s12a);
		assert (ms12a.toDisplayString().equals(s12));
		
		System.out.println("MerString passed");
	}
}
