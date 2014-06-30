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

import java.util.ArrayList;

// Confidence tests for the BasicAssembler class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class BasicAssemblerTest {

	public static void test() {
		System.out.println("Testing BasicAssembler:");

		ArrayList<String> reads = new ArrayList<String>();
		reads.add("ATG");
		reads.add("GTG");
		reads.add("CGT");
		reads.add("GCG");
		reads.add("TGC");
		reads.add("GCA");
		reads.add("TGG");
		reads.add("GGC");
		
		BasicAssembler assembler = new BasicAssembler(reads, 2);
		ArrayList<String> results = assembler.assemble();
		
		assert (results.size() == 1);
		String seq = results.get(0);
		
		System.out.println(seq);
		
		assert ((seq.equals("ATGGCGTGCA")) || (seq.equals("ATGCGTGGCA")));

		System.out.println("BasicAssembler passed.");
	}
	
}
