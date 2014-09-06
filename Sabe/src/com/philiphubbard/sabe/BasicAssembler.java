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

import java.util.ArrayDeque;
import java.util.ArrayList;

import com.philiphubbard.digraph.BasicDigraph;
import com.philiphubbard.digraph.Digraph;
import com.philiphubbard.digraph.EulerPaths;

// A class to assemble genomic sequences from a list of "reads".
// The algorithm breaks each read string into substrings of length
// k-1.  These (k-1)-mers are the vertices of a De Bruijn graph,
// and the edges correspond to the k-mers that are the overlap of
// the adjacent substrings of length k-1.  The sequence is assembled
// by finding the Euler tour of this graph, the tour that visits each
// edge exactly once.  (Hence the name of the package, "Sequence Assembly
// By Euler tours.")  This implementation does not use Hadoop, and is
// very basic in that it:
// * does not handle errors;
// * does not handle repeats (longer than k-1 base pairs);
// * does not handle graphs that do not have a complete Euler tour.

public class BasicAssembler {
	
	// Constructor.  Builds the graph from the list of reads.
	// The length of the substrings that define the vertices, k-1, is
	// vertexMerLength.
	
	public BasicAssembler(ArrayList<String> reads, int vertexMerLength) {
		this.vertexMerLength = vertexMerLength;
		int vertexCapacity = 0x1 << (2 * vertexMerLength);
		
		graph = new BasicDigraph(vertexCapacity, Digraph.EdgeMultiples.ENABLED);
		populateGraph(reads);
	}
	
	// Assemble and return the sequence(s) from the graph built in the constructor.
	
	public ArrayList<String> assemble() {
		ArrayList<String> result = new ArrayList<String>();
		
		EulerPaths<BasicDigraph.Edge> euler = new EulerPaths<BasicDigraph.Edge>(graph);
		ArrayList<ArrayDeque<Integer>> paths = euler.getPaths();
		
		for (ArrayDeque<Integer> path : paths) {			
			if (addedSinkSourceEdge)
				path.pollLast();
			
			String seq = new String();
			for (int v : path) {
				if (seq.length() == 0)
					seq = Mer.fromInt(v, vertexMerLength);
				else
					seq = seq.concat(Mer.fromInt(v, 1));
			}
			
			result.add(seq);
		}
		
		return result;
	}
	
	//
	
	private void populateGraph(ArrayList<String> reads) {
		for (String read : reads) {
			if (read.length() < vertexMerLength)
				continue;
			int prev = -1;
			for (int i = 0; i < read.length() - vertexMerLength + 1; i++) {
				String mer = read.substring(i, i + vertexMerLength);
				int curr = Mer.toInt(mer);
				if (prev != -1)
					graph.addEdge(prev, new BasicDigraph.Edge(curr));
				prev = curr;
			}
		}
		
		int source = -1;
		int sink = -1;
		
		boolean multipleSources = false;
		boolean multipleSinks = false;
		
		for (int v = 0; v < graph.getVertexCapacity(); v++) {
			if (graph.getInDegree(v) == 0) {
				if (source != -1)
					multipleSources = true;
				source = v;
			}
			if (graph.getOutDegree(v) == 0) {
				if (sink != -1)
					multipleSinks = true;
				sink = v;
			}
		}
		
		assert (!multipleSources && !multipleSinks);
		
		addedSinkSourceEdge = ((source != -1) && (sink != -1));
		if (addedSinkSourceEdge)
			graph.addEdge(sink, new BasicDigraph.Edge(source));
	}

	private int vertexMerLength;
	private BasicDigraph graph;
	boolean addedSinkSourceEdge;
	
}
