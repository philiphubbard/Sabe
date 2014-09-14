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

import java.lang.Math;
import java.util.ArrayDeque;
import java.util.ArrayList;

import com.philiphubbard.digraph.BasicDigraph;
import com.philiphubbard.digraph.StrongComponents;

// A class to clean up or rectify a digraph.BasicDigraph with edge multiples that 
// represents Euler paths with repetitive sections, also known as "repeats".
// Currently, the code handles only "single repeats", which are sections that are
// repeated exactly once (i.e., that occur as two instances); for example, in the 
// path A -> X -> Y -> Z -> B -> X -> Y -> Z -> C, the single repeat is the section
// X -> Y -> Z.  The graph containing this single repeat will have edge multiples, 
// two edges from X to Y and from Y to Z.  Note that a graph can contain multiple 
// distinct single repeats.
//
// Rectification of single repeats may be necessary in a graph that has both a 
// coverage greater than one and the possibility of errors.  "Coverage" refers to the 
// expected number of edge multiples, which is assumed to be uniform and consistent 
// over the graph.  Errors involve parts of the graph where the edge multiples are 
// fewer than expected.  For example, if coverage is three then a graph should have
// a form like A -3-> B -3-> C -6-> D -6-> E -3-> G and E -3-> F -3-> C, where "-n->" 
// indicates n edge multiples and where C -6-> D -6-> E is a single repeat; but an 
// error might cause the graph to start with A -2-> B -2-> C and A -1-> H -1-> C, where 
// H is an error.  One approach to handling errors will eliminate parts of the graph with 
// coverage less than ceiling(coverage / 2.0); this value is ceiling(3/2.0) or 2 in the 
// previous example.  The input to rectification is then a graph with varying edge 
// multiples, like A -2-> B -2-> C -6-> D -5-> E -3-> G and E -3-> F -3-> C.  The goal 
// of rectification is to make the graph have edge multiples of one everywhere except on 
// single repeats, where the edge multiple will be two.
//
// The rectification algorithm makes two passes over the graph.  The first pass finds the
// strongly-connected components or "strong components" of the graph; vertices V and W 
// are in the same strong component if there is a path from V to W and from W to V.  The
// second pass looks for single repeats between branch vertices, and sets the edge
// multiples to be two in those cases and one everywhere else.  Both have processing time
// that is linear in the size of the graph (size being the number of vertices plus the
// number of edges).

public class Repeats {
	
	// Rectify the specified graph with the specified coverage.  The boolean array 
	// should have a true value at the index of each branch vertex.
	
	public static void rectifySingle(BasicDigraph graph, int coverage, boolean[] isBranch) {
		
		// The algorithm identifies pairs of branch vertices, B1 and B2, that have
		// sufficient edge multiples on the path from B1 to B2 that the path could be
		// a single repeat.  But to really be a single repeat, there must be another
		// path the "loops back" from B2 to B1 to start the repeat.  So there must be 
		// both a path from B1 to B2 and from B2 to B1, and thus B1 and B2 must be in 
		// the same strong component.  So start by finding all the strong components.
		
		StrongComponents<BasicDigraph.Edge> strongComps = 
				new StrongComponents<BasicDigraph.Edge>(graph);
		
		// The algorithm is based on depth-first search, so we need to keep track of
		// whether a vertex was visited, to avoid cycling endlessly.
		
		boolean[] wasVisited = new boolean[graph.getVertexCapacity()];
		for (int i = 0; i < wasVisited.length; i++)
			wasVisited[i] = false;
		
		// The State class keeps track of information about a vertex that we need to
		// determine if it is part of a single repeat.  As the depth-first search
		// proceeds, the stack of State instances keeps track of vertices that have
		// been visited but are not fully processed.  The stack must be implemented
		// with a deque because part of the checking for a single repeat involves
		// stepping back through elements still on the stack, and only a deque supports
		// that operation.
		
		ArrayDeque<State> stack = new ArrayDeque<State>();
		
		// Start the search with the initial source vertex.  Mark it as a possible end
		// of a single repeat.
		
		stack.push(new State(graph, 0, 0));
		stack.peek().isEnd = true;
		
		while (!stack.isEmpty()) {
			
			// Get the current vertex state from the stack, but do not remove it.  We
			// remove it only when we are done iterating over all its edges.
			
			State state = stack.peek();
			
			ArrayList<BasicDigraph.Edge> edges = state.next();
			if (!state.done()) {
				
				// If we have not finished iterating over all the edges from the current
				// vertex, get the next one.
				
				int vertex = edges.get(0).getTo();
				
				// Go ahead and add to the stack a State instance for the vertex pointed to
				// by the edge.  That way, the checking for a single repeat need only process
				// vertices on the stack.
				
				stack.push(new State(graph, vertex, edges.size()));
				
				if (isBranch[vertex]) {
					
					// If the vertex is a branch, then walk back along the edges from it
					// to the last vertex marked as possibly being the end of a single
					// repeat.

					if (isRepeat(stack, isBranch, strongComps, coverage)) {
						// If these edges are consistent with a single repeat, then walk
						// back to that end vertex again, forcing the edge multiples along
						// that path to two.
						
						forceEdgeMultiple(2, stack, graph);
					}
					else {
						// Otherwise, the path back to that end vertex is not a single
						// repeat, so walk back to it and force the edge multiples to one.
						
						forceEdgeMultiple(1, stack, graph);
					}
					
					// The current vertex now is the new possible end of a single repeat.
					
					stack.peek().isEnd = true;
				}
				else if (graph.isSink(vertex)) {
					
					// If the current vertex is not a branch but is the final sink, then the
					// path back to the last vertex marked as an end is not a single repeat,
					// and the edge multiples on that path should be one.
					
					forceEdgeMultiple(1, stack, graph);
				}
				
				// If the vertex we just got to by an edge has already been visited, we can now
				// remove it from the stack, now that we are done with the code that is most
				// easily phrased as operating only on stack vertices.
				
				if (wasVisited[vertex]) 
					stack.pop();
				else
					wasVisited[vertex] = true;
			}
			else {
				
				// If we have finished iterating over all the edges from the current
				// vertex, then pop it from the stack.
				
				stack.pop();
			}
		}
	}
	
	//
	
	// Returns true if the path from the top of the stack back to the last vertex marked
	// as an end consists of edges consistent with a single repeat.
	
	private static boolean isRepeat(ArrayDeque<State> stack, boolean[] isBranch,
			StrongComponents<BasicDigraph.Edge> strongComps, int coverage) {
		
		// For a specified coverage, there must be ceiling(coverage / 2.0) edges to 
		// indicate no error for this edge.  A single repeat corresponds to two
		// non-erroneous passes along this paths.
		
		int minEdgeMultiple = 2 * (int) Math.ceil(coverage / 2.0);
		
		// Remember the vertex at the top of the stack, so we can compare it to the other
		// vertices as we walk back through the stack.
		
		int vertex = stack.peek().vertex;
		for (State state : stack) {
			
			if (state.isEnd) {
				
				// If the other vertex in the stack is marked as a possible end of a
				// single repeat, then the path back to it is actually a single repeat
				// if it is a branch and if it and the vertex at the top of the stack
				// are in the same strong component.  Being in the same strong component
				// means there is another path that "loops back" to start the repeat
				// again.  We don't need to consider the edge multiples on that
				// looping-back path because it is not itself part of the repeat.
				
				return (isBranch[state.vertex] && 
						strongComps.isStronglyReachable(vertex, state.vertex));
			}
			else if (state.edgeMultiple < minEdgeMultiple) {
				
				// If at any point in walking back through the stack we find edge multiples
				// that are too low to be part of a single repeat, then we can stop and
				// conclude that we have not found a repeat.
				
				return false;
			}
		}
		
		return false;
	}
	
	// Force the path from the top of the stack back to the last vertex marked as an end
	// to have the specified edge multiple.
	
	private static void forceEdgeMultiple(int edgeMultiple, ArrayDeque<State> stack, 
			BasicDigraph graph) {		
		State prev = null;
		for (State curr : stack) {
			if (prev != null) {
				if (prev.edgeMultiple > edgeMultiple) {
					int n = prev.edgeMultiple - edgeMultiple;
					for (int i = 0; i < n; i++)
						graph.removeEdge(curr.vertex, prev.vertex);
				}				
			}
			prev = curr;
			if (curr.isEnd)
				break;
		}
	}
	
	// The state of the processing of a vertex, to be stored on the stack.
	
	private static class State {
		State(BasicDigraph graph, int vertex, int edgeMultiple) {
			this.vertex = vertex;
			this.edgeMultiple = edgeMultiple;
			this.graph = graph;
			isEnd = false;
		}
		
		// Returns true if the iteration over the vertex's edges is done.
		
		boolean done() {
			return ((it != null) && it.done());
		}
		
		// Returns the next step of the iteration over the vertex's edges.
		// The result is a list of edges pointing to a common vertex.
		
		ArrayList<BasicDigraph.Edge> next() {
			if (it == null) {
				it = graph.createAdjacencyMultipleIterator(vertex);
				return it.begin();
			}
			else {
				return it.next();
			}
		}
		
		int vertex;
		int edgeMultiple;
		boolean isEnd;
		
		BasicDigraph graph;
		BasicDigraph.AdjacencyMultipleIterator it;
	}
}
