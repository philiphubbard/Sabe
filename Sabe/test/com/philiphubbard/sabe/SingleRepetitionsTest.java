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
import java.util.Arrays;
import java.util.List;

import com.philiphubbard.digraph.BasicDigraph;
import com.philiphubbard.digraph.Digraph;

public class SingleRepetitionsTest {
	
	public static void test() {
		System.out.println("Testing SingleRepetitions:");
		
		test1();
		test2();
		test3();

		System.out.println("SingleRepetitions passed.");
	}
	
	private static void test1() {
		System.out.println("SingleRepetitions test 1:");
		
		BasicDigraph graph = new BasicDigraph(6, Digraph.EdgeMultiples.ENABLED);
		
		// A simple graph with single coverage (so single repetitions involve two edges)
		// and no errors.
		
		int coverage = 1;
		
		graph.addEdge(0, new BasicDigraph.Edge(1));
		
		// 1->2 is a repetition.  The loop-back path to start the second instance is
		// 2->3->4->3->4->1; note that the loop-back path has a repetition, too.
		
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		
		graph.addEdge(2, new BasicDigraph.Edge(3));
		
		// 3->4 is a repetition.  The loop-back path is 4->3.
		
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		
		graph.addEdge(4, new BasicDigraph.Edge(3));
		
		graph.addEdge(4, new BasicDigraph.Edge(1));
		
		graph.addEdge(2, new BasicDigraph.Edge(5));
		
		boolean[] isBranch = new boolean[6];
		isBranch[0] = false;
		isBranch[1] = true;
		isBranch[2] = true;
		isBranch[3] = true;
		isBranch[4] = true;
		isBranch[5] = false;
		
		SingleRepetitions.rectify(graph, coverage, isBranch);
		
		assert (edgesEqual(graph, 0, Arrays.asList(1)));
		assert (edgesEqual(graph, 1, Arrays.asList(2, 2)));
		assert (edgesEqual(graph, 2, Arrays.asList(3, 5)));
		assert (edgesEqual(graph, 3, Arrays.asList(4, 4)));
		assert (edgesEqual(graph, 4, Arrays.asList(1, 3)));
		assert (edgesEqual(graph, 5, null));
		
		System.out.println("SingleRepetitions test 1 passed.");
	}

	private static void test2() {
		System.out.println("SingleRepetitions test 2:");

		BasicDigraph graph = new BasicDigraph(6, Digraph.EdgeMultiples.ENABLED);
		
		// The same graph as in test1, but with increased coverage, and some errors.
		
		int coverage = 3;

		// One error: one edge missing (so coverage - 1 edges for a segment that is not repeated).
		
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(0, new BasicDigraph.Edge(1));
		
		// No errors on the repetition 1->2.  Recall that the loop-back path is
		// 2->3->4->3->4->1 (containing a repetition itself).
		
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		
		// No errors.
		
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		
		// Two errors: two edges missing from the repetition 3->4.
		
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		
		// One error: one edge missing.
		
		graph.addEdge(4, new BasicDigraph.Edge(3));
		graph.addEdge(4, new BasicDigraph.Edge(3));
		
		// No errors.
		
		graph.addEdge(4, new BasicDigraph.Edge(1));
		graph.addEdge(4, new BasicDigraph.Edge(1));
		graph.addEdge(4, new BasicDigraph.Edge(1));
		
		// One error: one edge missing.
		
		graph.addEdge(2, new BasicDigraph.Edge(5));
		graph.addEdge(2, new BasicDigraph.Edge(5));
		
		boolean[] isBranch = new boolean[6];
		isBranch[0] = false;
		isBranch[1] = true;
		isBranch[2] = true;
		isBranch[3] = true;
		isBranch[4] = true;
		isBranch[5] = false;
		
		SingleRepetitions.rectify(graph, coverage, isBranch);
		
		assert (edgesEqual(graph, 0, Arrays.asList(1)));
		assert (edgesEqual(graph, 1, Arrays.asList(2, 2)));
		assert (edgesEqual(graph, 2, Arrays.asList(3, 5)));
		assert (edgesEqual(graph, 3, Arrays.asList(4, 4)));
		assert (edgesEqual(graph, 4, Arrays.asList(1, 3)));
		assert (edgesEqual(graph, 5, null));
		
		System.out.println("SingleRepetitions test 2 passed.");
	}

	private static void test3() {
		System.out.println("SingleRepetitions test 3:");
		
		BasicDigraph graph = new BasicDigraph(17, Digraph.EdgeMultiples.ENABLED);
		
		// A more complex graph, with coverage of 3.
		
		int coverage = 3;
		
		// No errors.
		
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(0, new BasicDigraph.Edge(1));
		
		// One error: one missing edge.
		
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		
		// One error: one edge missing from the first part of the repetition 2->3->4.  
		// The loop-back path is 4->5->6->2.
		
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		
		// No errors in the second part of the repetition 2->3->4.
		
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(4));
		
		// One error: one missing edge.
		
		graph.addEdge(4, new BasicDigraph.Edge(5));
		graph.addEdge(4, new BasicDigraph.Edge(5));
		
		// One error: one extra edge.
		
		graph.addEdge(5, new BasicDigraph.Edge(6));
		graph.addEdge(5, new BasicDigraph.Edge(6));
		graph.addEdge(5, new BasicDigraph.Edge(6));
		graph.addEdge(5, new BasicDigraph.Edge(6));
		
		// No errors.
		
		graph.addEdge(6, new BasicDigraph.Edge(2));
		graph.addEdge(6, new BasicDigraph.Edge(2));
		graph.addEdge(6, new BasicDigraph.Edge(2));
		
		// No errors on the first part of the repetition 4->7->8.  
		// The loop-back path is 8->9->10->4.
		
		graph.addEdge(4, new BasicDigraph.Edge(7));
		graph.addEdge(4, new BasicDigraph.Edge(7));
		graph.addEdge(4, new BasicDigraph.Edge(7));
		graph.addEdge(4, new BasicDigraph.Edge(7));
		graph.addEdge(4, new BasicDigraph.Edge(7));
		graph.addEdge(4, new BasicDigraph.Edge(7));
		
		// Two errors: two missing edges the second part of the repetition
		// 4->7->8.
		
		graph.addEdge(7, new BasicDigraph.Edge(8));
		graph.addEdge(7, new BasicDigraph.Edge(8));
		graph.addEdge(7, new BasicDigraph.Edge(8));
		graph.addEdge(7, new BasicDigraph.Edge(8));
		
		// No errors.
		
		graph.addEdge(8, new BasicDigraph.Edge(9));
		graph.addEdge(8, new BasicDigraph.Edge(9));
		graph.addEdge(8, new BasicDigraph.Edge(9));
		
		// No errors.
		
		graph.addEdge(9, new BasicDigraph.Edge(10));
		graph.addEdge(9, new BasicDigraph.Edge(10));
		graph.addEdge(9, new BasicDigraph.Edge(10));
		
		// One error: one missing edge.
		
		graph.addEdge(10, new BasicDigraph.Edge(4));
		graph.addEdge(10, new BasicDigraph.Edge(4));
		
		// No errors.
		
		graph.addEdge(8, new BasicDigraph.Edge(11));
		graph.addEdge(8, new BasicDigraph.Edge(11));
		graph.addEdge(8, new BasicDigraph.Edge(11));
		
		// One error: one missing edge.  Note that 12 is the sink.
		
		graph.addEdge(11, new BasicDigraph.Edge(12));
		graph.addEdge(11, new BasicDigraph.Edge(12));
		
		// No errors on the first part of the repetition
		// 8->13->14->15->16.  Note that this repetition is its own
		// loop-back path to 8.
		
		graph.addEdge(8, new BasicDigraph.Edge(13));
		graph.addEdge(8, new BasicDigraph.Edge(13));
		graph.addEdge(8, new BasicDigraph.Edge(13));
		graph.addEdge(8, new BasicDigraph.Edge(13));
		graph.addEdge(8, new BasicDigraph.Edge(13));
		graph.addEdge(8, new BasicDigraph.Edge(13));
		
		// No errors.
		
		graph.addEdge(13, new BasicDigraph.Edge(14));
		graph.addEdge(13, new BasicDigraph.Edge(14));
		graph.addEdge(13, new BasicDigraph.Edge(14));
		graph.addEdge(13, new BasicDigraph.Edge(14));
		graph.addEdge(13, new BasicDigraph.Edge(14));
		graph.addEdge(13, new BasicDigraph.Edge(14));
		
		// One error: one extra edge for this repetition.
		
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		graph.addEdge(14, new BasicDigraph.Edge(15));
		
		// One error: one edge missing from this repetition.
		
		graph.addEdge(15, new BasicDigraph.Edge(16));
		graph.addEdge(15, new BasicDigraph.Edge(16));
		graph.addEdge(15, new BasicDigraph.Edge(16));
		graph.addEdge(15, new BasicDigraph.Edge(16));
		graph.addEdge(15, new BasicDigraph.Edge(16));
		
		// Two errors: two edges missing from this repetition.
		
		graph.addEdge(16, new BasicDigraph.Edge(8));
		graph.addEdge(16, new BasicDigraph.Edge(8));
		graph.addEdge(16, new BasicDigraph.Edge(8));
		graph.addEdge(16, new BasicDigraph.Edge(8));
		
		boolean[] isBranch = new boolean[17];
		isBranch[0] = false;
		isBranch[1] = false;
		isBranch[2] = true;
		isBranch[3] = false;
		isBranch[4] = true;
		isBranch[5] = false;
		isBranch[6] = false;
		isBranch[7] = false;
		isBranch[8] = true;
		isBranch[9] = false;
		isBranch[10] = false;
		isBranch[11] = false;
		isBranch[12] = false;
		isBranch[13] = false;
		isBranch[15] = false;
		isBranch[16] = false;
		
		SingleRepetitions.rectify(graph, coverage, isBranch);
		
		assert (edgesEqual(graph, 0, Arrays.asList(1)));
		assert (edgesEqual(graph, 1, Arrays.asList(2)));
		assert (edgesEqual(graph, 2, Arrays.asList(3, 3)));
		assert (edgesEqual(graph, 3, Arrays.asList(4, 4)));
		assert (edgesEqual(graph, 4, Arrays.asList(5, 7, 7)));
		assert (edgesEqual(graph, 5, Arrays.asList(6)));
		assert (edgesEqual(graph, 6, Arrays.asList(2)));
		assert (edgesEqual(graph, 7, Arrays.asList(8, 8)));
		assert (edgesEqual(graph, 8, Arrays.asList(9, 11, 13, 13)));
		assert (edgesEqual(graph, 9, Arrays.asList(10)));
		assert (edgesEqual(graph, 10, Arrays.asList(4)));
		assert (edgesEqual(graph, 11, Arrays.asList(12)));
		assert (edgesEqual(graph, 12, null));
		assert (edgesEqual(graph, 13, Arrays.asList(14, 14)));
		assert (edgesEqual(graph, 14, Arrays.asList(15, 15)));
		assert (edgesEqual(graph, 15, Arrays.asList(16, 16)));
		assert (edgesEqual(graph, 16, Arrays.asList(8, 8)));

		System.out.println("SingleRepetitions test 3 passed.");
	}
	
	// Returns true if the specified vertex in the specified graph has edges to the expected
	// list of vertex IDs, which should be sorted in ascending order.
	
	private static boolean edgesEqual(BasicDigraph graph, int vertex, List<Integer> expected) {
		ArrayList<Integer> actual = new ArrayList<Integer>();
		BasicDigraph.AdjacencyIterator it = graph.createAdjacencyIterator(vertex);
		for (BasicDigraph.Edge edge = it.begin(); !it.done(); edge = it.next())
			actual.add(edge.getTo());
		if (expected == null)
			return actual.isEmpty();
		else
			return actual.equals(expected);
	}

}
