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
import java.util.ArrayDeque;
import java.lang.Math;

import com.philiphubbard.digraph.BasicDigraph;
import com.philiphubbard.digraph.StrongComponents;

public class SingleRepetitions {
	
	public static void rectify(BasicDigraph graph, int coverage, boolean[] isBranch) {
		StrongComponents<BasicDigraph.Edge> strongComps = 
				new StrongComponents<BasicDigraph.Edge>(graph);
		
		boolean[] visited = new boolean[graph.getVertexCapacity()];
		for (int i = 0; i < visited.length; i++)
			visited[i] = false;
		
		ArrayDeque<State> stack = new ArrayDeque<State>();
		stack.push(new State(graph, 0, 0));
		stack.peek().isEnd = true;
		
		while (!stack.isEmpty()) {
			State state = stack.peek();
			
			ArrayList<BasicDigraph.Edge> edges = state.next();
			if (!state.done()) {
				int vertex = edges.get(0).getTo();
				
				stack.push(new State(graph, vertex, edges.size()));
				
				if (isBranch[vertex]) {
					if (isFeasibleRepetition(stack, isBranch, strongComps, coverage)) {
						forceMultiplicity(2, stack, graph);
					}
					else {
						forceMultiplicity(1, stack, graph);
					}
					stack.peek().isEnd = true;
				}
				else if (graph.isSink(vertex)) {
					forceMultiplicity(1, stack, graph);
				}
				
				if (visited[vertex]) 
					stack.pop();
				else
					visited[vertex] = true;
			}
			else {
				stack.pop();
			}
		}
	}
	
	//
	
	private static boolean isFeasibleRepetition(ArrayDeque<State> stack, boolean[] isBranch,
			StrongComponents<BasicDigraph.Edge> strongComps, int coverage) {
		
		// For a specified coverage, there must be ceiling(coverage / 2) edges to 
		// indicate no error for this edge.  A single repetition corresponds to 
		// two non-erroneous passes along this paths.
		
		int minMultiplicity = 2 * (int) Math.ceil(coverage / 2.0);
		
		int vertex = stack.peek().vertex;
		for (State state : stack) {
			if (state.isEnd) {
				return (isBranch[state.vertex] && 
						strongComps.isStronglyReachable(vertex, state.vertex));
			}
			else if (state.multiplicity < minMultiplicity) {
				return false;
			}
		}
		
		return false;
	}
	
	private static void forceMultiplicity(int multiplicity, ArrayDeque<State> stack, 
			BasicDigraph graph) {
		State prev = null;
		for (State curr : stack) {
			if (prev != null) {
				if (prev.multiplicity > multiplicity) {
					int n = prev.multiplicity - multiplicity;
					for (int i = 0; i < n; i++)
						graph.removeEdge(curr.vertex, prev.vertex);
				}				
			}
			prev = curr;
			if (curr.isEnd)
				break;
		}
	}
	
	private static class State {
		public State(BasicDigraph graph, int vertex, int multiplicity) {
			this.vertex = vertex;
			this.multiplicity = multiplicity;
			this.graph = graph;
			isEnd = false;
		}
		
		public boolean done() {
			return ((it != null) && it.done());
		}
		
		public ArrayList<BasicDigraph.Edge> next() {
			if (it == null) {
				it = graph.createAdjacencyMultipleIterator(vertex);
				return it.begin();
			}
			else {
				return it.next();
			}
		}
		
		// HEY!! Accessor functions instead of public members? Cannot hide private members?
		public int vertex;
		public int multiplicity;
		public boolean isEnd;
		
		private BasicDigraph graph;
		private BasicDigraph.AdjacencyMultipleIterator it;
	}
}