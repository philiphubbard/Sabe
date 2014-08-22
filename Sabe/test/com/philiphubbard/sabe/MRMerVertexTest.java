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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import com.philiphubbard.digraph.MRVertex;

public class MRMerVertexTest {

	public static void test() {
		System.out.println("Testing MRMerVertex:");
		
		Configuration config = new Configuration();
		config.setInt(MRMerVertex.CONFIG_MER_LENGTH, 5);
		
		String s1 = "ACGTA";
		int m1 = Mer.toInt(s1);
		MRMerVertex mv1 = new MRMerVertex(m1, config);
		
		String s2 = "CGTAC";
		int m2 = Mer.toInt(s2);
		MRMerVertex mv2 = new MRMerVertex(m2, config);

		String s3 = "GTACG";
		int m3 = Mer.toInt(s3);
		MRMerVertex mv3 = new MRMerVertex(m3, config);
		
		String s4 = "TACGT";
		int m4 = Mer.toInt(s4);
		MRMerVertex mv4 = new MRMerVertex(m4, config);
		
		String s5 = "ACGTT";
		int m5 = Mer.toInt(s5);
		
		mv1.addEdgeTo(m2);
		mv2.addEdgeTo(m3);
		mv3.addEdgeTo(m4);
		mv4.addEdgeTo(m5);
		
		mv1.compressChain(mv2, config);
		
		String s1_2 = "ACGTAC";
		int m1_2 = Mer.toInt(s1_2);
		MerString ms1_2 = new MerString(m1_2, 6);
		
		assert (mv1.getMerString().equals(ms1_2));
		
		try {
			BytesWritable t1_2 = mv1.toWritable(MRVertex.EdgeFormat.EDGES_TO);
			MRMerVertex mv1a = new MRMerVertex(t1_2, config);
			
			assert (mv1a.equals(mv1));
			assert (mv1a.getMerString().equals(ms1_2));
			
			mv3.compressChain(mv4, config);
			
			String s3_4 = "GTACGT";
			int m3_4 = Mer.toInt(s3_4);
			MerString ms3_4 = new MerString(m3_4, 6);
			
			assert (mv3.getMerString().equals(ms3_4));
			
			BytesWritable t3_4 = mv3.toWritable(MRVertex.EdgeFormat.EDGES_TO);
			MRMerVertex mv3a = new MRMerVertex(t3_4, config);
			
			assert (mv3a.equals(mv3));
			assert (mv3a.getMerString().equals(ms3_4));
			
			mv1.compressChain(mv3, config);
			
			String s1_4 = "ACGTACGT";
			int m1_4 = Mer.toInt(s1_4);
			MerString ms1_4 = new MerString(m1_4, 8);
			
			assert (mv1.getMerString().equals(ms1_4));
			
			BytesWritable t1_4 = mv1.toWritable(MRVertex.EdgeFormat.EDGES_TO);
			MRMerVertex mv1b = new MRMerVertex(t1_4, config);
			
			assert (mv1b.equals(mv1));
			assert (mv1b.getMerString().equals(ms1_4));
			
			//
			
			config.setInt(MRMerVertex.CONFIG_MER_LENGTH, 3);
			
			int m10 = Mer.toInt("TCG");
			MRMerVertex mv10 = new MRMerVertex(m10, config);
			
			BytesWritable t10a = mv10.toWritable(MRVertex.EdgeFormat.EDGES_TO);
			MRMerVertex mv10a = new MRMerVertex(t10a, config);
			
			System.out.println("mv10a " + mv10a.toDisplayString());
	
			
			int m11 = Mer.toInt("CGA");
			MRMerVertex mv11 = new MRMerVertex(m11, config);
	
			mv10.addEdgeTo(m11);
			mv11.addEdgeTo(Mer.toInt("GAG"));
			mv10.compressChain(mv11, config);
			
			BytesWritable t10b = mv10.toWritable(MRVertex.EdgeFormat.EDGES_TO);
			MRMerVertex mv10b = new MRMerVertex(t10b, config);
			
			assert (mv10.equals(mv10b));
		}
		catch (IOException exception) {
			System.out.println(exception.getMessage());
			assert (false);
		}
		
		System.out.println("MRMerVertex passed.");
	}

}
