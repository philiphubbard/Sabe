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

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import com.philiphubbard.digraph.MRCollectVertexEdges;
import com.philiphubbard.digraph.MRVertex;

// A mapper and reducer for a Hadoop map-reduce algorithm to convert a set of
// "read" strings into a De Bruijn graph whose vertices are the (k-1) mer substrings
// of the reads and whose edges correspond to the k-mer overlaps between adjacent
// substrings.  The output is a file containing strings for the MRVertex representation
// of the graph.  This class builds on the MRCollectVertexEdges class.

public class MRBuildVertices extends MRCollectVertexEdges {
	
	public static void setVertexMerLength(int length) {
		vertexMerLength = length;
	}
	
	public static int getVertexMerLength() {
		return vertexMerLength;
	}
	
	// The mapper simply overrides the verticesFromInputValue() function of the
	// MRCollectVertices.Mapper class, to take input in the form of "read" strings.
	
	public static class Mapper extends MRCollectVertexEdges.Mapper {
		
		protected ArrayList<MRVertex> verticesFromInputValue(Text value) {
			ArrayList<MRVertex> result = new ArrayList<MRVertex>();
			
			String s = value.toString();
			MRVertex prev = null;
			for (int i = 0; i < s.length() - vertexMerLength + 1; i++) {
				String mer = s.substring(i, i + vertexMerLength);
				int id = Mer.toInt(mer);
				MRVertex curr = new MRVertex(id);
				result.add(curr);
				if (prev != null)
					prev.addEdgeTo(id);
				prev = curr;
			}
			
			return result;
		}

	}
	
	// The MRCollectVerteEdges.Reducer class can be used as is.
	
	//
	
	private static int vertexMerLength = 4;

}
