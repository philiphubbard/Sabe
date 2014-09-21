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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;

import com.philiphubbard.digraph.BasicDigraph;
import com.philiphubbard.digraph.Digraph;
import com.philiphubbard.digraph.EulerPaths;
import com.philiphubbard.digraph.MRBuildVertices;
import com.philiphubbard.digraph.MRCompressChains;
import com.philiphubbard.digraph.MRVertex;

// A class to assemble genomic sequences from a list of "reads" using
// MapReduce (MR) algorithms implemented in Hadoop to improve performance.
//
// The algorithm uses one MapReduce pass to break each read string into
// substrings of length k-1.  These (k-1)-mers are the vertices of a
// De Bruijn graph, and the edges correspond to the k-mers that are the
// overlap of the adjacent substrings of length k-1.  The graph is then
// reduced in size using a sequence of MapReduce passes that compress 
// linear chains of edges, which are expected to be very long in practice.  
// The final sequence is assembled from the reduced graph by a sequential
// algorithm to find the Euler tour, the tour that visits each edge exactly
// once.  (Hence the name of the package, "Sequence Assembly By Euler tours.")
//
// The implementation includes basic techniques to handle errors in the
// initial reads and repeated substrings or "repeates" in the sequence.  Both 
// techniques depend on the initial reads containing duplicate coverage for 
// the entire sequence.  Duplicate coverage causes the graph to contain edge 
// multiples.  Error handling involves discarding vertices with fewer edges 
// than would be expected given the coverage.  Repeat handling involves making 
// two extra sequential passes over the reduced graph to identify chains whose 
// edge multiples indicate they must be repeats given the coverage.

public class MRAssembler {
	
	// Construct the assembler.  The vertexMerLength is the k-1 in the 
	// (k-1)-mers that define the graph vertices.  The coverage is the number
	// of times each part of the final sequence will be covered by the union
	// of all the reads; coverage is assumed to be constant and uniform over
	// the sequence.

	public MRAssembler(int vertexMerLength, int coverage) {
		this.vertexMerLength = vertexMerLength;
		this.coverage = coverage;
	}
	
	// Run the MapReduce passes and sequential algorithms that perform the 
	// sequence assembly.  The inputPath is a directory, all of whose files
	// contain read strings, one read per line (ending with "\n" character).
	// The outputPath is a directory in which a file with the final assembled
	// sequence will be created.  A temporary directory named "sabe.MRAssemblerTmp"
	// will be created in the current working directory to hold intermediate
	// results from the MapReduce passes.
	
	public boolean run(Path inputPath, Path outputPath) 
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		// Job.getInstance() copies the Configuration argument, so set its properties first.
		
		conf.setBoolean(MRVertex.CONFIG_ALLOW_EDGE_MULTIPLES, true);
		conf.setBoolean(MRVertex.CONFIG_COMPRESS_CHAIN_MULTIPLES_MUST_MATCH, false);
		conf.setInt(MRMerVertex.CONFIG_MER_LENGTH, vertexMerLength);
		conf.setBoolean(MRBuildVertices.CONFIG_PARTITION_BRANCHES_CHAINS, true);
		conf.setInt(MRBuildVertices.CONFIG_COVERAGE, coverage);
		conf.setInt(MRCompressChains.CONFIG_TERMINATION_COUNT, 1);

		Job buildJob = Job.getInstance(conf);
		buildJob.setJobName("mrassemblerbuild");
		
		Path buildInputPath = inputPath;
		Path buildOutputPath = new Path("sabe.MRAssemblerTmp");

		System.out.println("sabe.MRAssembler starting vertex construction");

		MRBuildMerVertices.setupJob(buildJob, buildInputPath, buildOutputPath);	
		
		if (!buildJob.waitForCompletion(true))
			return false;
		
		//
		
		Path compressInputPath = new Path(buildOutputPath.toString() + "/chain");
		Path compressOutputPath = new Path(buildOutputPath.toString() + "/chainCompress");
		
		int iter = 0;
		boolean keepGoing = true;
		MRCompressChains.beginIteration();
		while (keepGoing) {
			Job compressJob = Job.getInstance(conf);
			compressJob.setJobName("mrassemblercompress");
			
			System.out.println("sabe.MRAssembler starting compression iteration " + iter);

			MRCompressMerChains.setupIterationJob(compressJob, compressInputPath, compressOutputPath);
			
			if (!compressJob.waitForCompletion(true))
				System.exit(1);
			
			iter++;
			keepGoing = MRCompressChains.continueIteration(compressJob, compressInputPath, compressOutputPath);
		}
		
		System.out.println("sabe.MRAssembler made " + iter + " compression iterations");
		
		//
		
		Path branchPath = new Path(buildOutputPath.toString() + "/branch");
		Path chainPath = compressOutputPath;
		
		FileSystem fileSystem = FileSystem.get(conf);
		
		Graph graph = buildCompressedGraph(conf, fileSystem, branchPath, chainPath);
		if (graph != null) {
			ArrayList<String> result = graph.assemble();
		
			FSDataOutputStream out = fileSystem.create(outputPath);
			for (String seq : result) {
				out.writeBytes(seq);
				out.writeBytes("\n");
			}
		}
		
		//
		
		fileSystem.delete(buildOutputPath, true);
		
		fileSystem.close();	

		return true;
	}
	
	// Build the graph after compressing chains.
	
	protected Graph buildCompressedGraph(Configuration conf, FileSystem fileSystem, 
			Path branchPath, Path chainPath) 
			throws IOException, InterruptedException {
		System.out.println("sabe.MRAssembler starting graph construction");

		ArrayList<MRMerVertex> vertices = new ArrayList<MRMerVertex>();
		
		FileStatus[] branchFiles = fileSystem.listStatus(branchPath);
		for (FileStatus status : branchFiles) 
			readVertices(status, vertices, conf);
		
		FileStatus[] chainFiles = fileSystem.listStatus(chainPath);
		for (FileStatus status : chainFiles)
			readVertices(status, vertices, conf);
		
		// Check for a malformed graph, that does not have exactly one source and sink.
		// Return null in this case.
		
		int numSources = 0;
		int numSinks = 0;
		for (MRMerVertex vertex : vertices) {
			if (vertex.getIsSource()) 
				numSources++;
			if (vertex.getIsSink())
				numSinks++;	
		}
		if ((numSources != 1) || (numSinks != 1)) {
			System.out.println("Malformed graph: number of sources = " + numSources
					+ ", number of sinks = " + numSinks);
			return null;
		}
		
		return new Graph(vertices);
	}

	// Read values from the specified FileStatus, create MRMerVertex instances from the values
	// and place them in the ArrayList.
	
	private void readVertices(FileStatus status, ArrayList<MRMerVertex> vertices, Configuration conf)
			throws IOException {
		Path path = status.getPath();
		if (path.getName().startsWith("part")) {
		    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		    IntWritable key = new IntWritable();
		    BytesWritable value = new BytesWritable();
		    while (reader.next(key, value))
		    	vertices.add(new MRMerVertex(value, conf));
		    reader.close();
		}
	}
	
	// A directed graph built from MRMerVertex instances.  The underlying representation
	// is a digraph.BasicDigraph.
	
	protected class Graph {
		
		// Construct the graph from the vertices.
		
		public Graph(ArrayList<MRMerVertex> vertices) {
			
			// The MRMerVertices have a wide range of IDs, since each ID is an encoding
			// of a (k-1)-mer.  For the digraph.BasicDigraph, vertices will have IDs
			// that are consecutive integers starting at 0.  These arrays map from
			// those integers to the Mers and MerStrings (resulting from chain compression)
			// for the vertices.
			
			mers = new int[vertices.size()];
			merStrings = new MerString[vertices.size()];
			
			// The vertices need to have their edges remapped, too.  This HashMap maps
			// from (k-1)-mer IDs to new digraph.BasicDigraph IDs to facilitate the
			// new edges.
			
			HashMap<Integer, Integer> merToIndex = new HashMap<Integer, Integer>(vertices.size());
			
			// The Repeats class needs an array indicating what vertices are branches 
			// (so it can avoid computing that status itself).
			
			boolean[] isBranch = new boolean[vertices.size()];
			
			MRMerVertex source = null;
			MRMerVertex sink = null;

			int i = 1;
			for (MRMerVertex vertex : vertices) {
				if (vertex.getIsSource()) {
					
					if (source == null)
						source = vertex;
				}
				
				if (vertex.getIsSink()) {
					
					if (sink == null)
						sink = vertex;
				}

				// The digraph.EulerPaths class expects the source to have index 0.
				int j = (vertex == source) ? 0 : i++;
				
				mers[j] = vertex.getId();
				merStrings[j] = vertex.getMerString();
				merToIndex.put(mers[j], j);
				isBranch[j] = vertex.getIsBranch();
			}
			
			// Create the graph of the vertices with the new IDs.
			
			graph = new BasicDigraph(vertices.size(), Digraph.EdgeMultiples.ENABLED);
			
			for (MRMerVertex vertex : vertices) {
				int j = merToIndex.get(vertex.getId());
				
				// Add the edges with the new IDs.
				
				MRVertex.AdjacencyIterator it = vertex.createToAdjacencyIterator();
				for (int to = it.begin(); !it.done(); to = it.next()) {
					BasicDigraph.Edge edge = new BasicDigraph.Edge(merToIndex.get(to).intValue());
					graph.addEdge(j, edge);
				}
			}
			
			System.out.println("sabe.MRAssembler starting rectification of repeats");
			
			Repeats.rectifySingle(graph, coverage, isBranch);

			// There must be an edge from the sink to the source for
			// digraph.EulerPaths to work correctly.
			
			addedSinkSourceEdge = false;
			if ((source != null) && (sink != null)) {
				int from = merToIndex.get(sink.getId()).intValue();
				int to = merToIndex.get(source.getId()).intValue();
				graph.addEdge(from, new BasicDigraph.Edge(to));
				addedSinkSourceEdge = true;
			}
		}
		
		// Assemble the final sequence and return it as a String.  There ought to be
		// jus one String, but the return value allows for the possibility of more
		// than one, just in case.
		
		public ArrayList<String> assemble() {
			System.out.println("sabe.MRAssembler starting final assembly");

			ArrayList<String> result = new ArrayList<String>();
			
			EulerPaths<BasicDigraph.Edge> euler = new EulerPaths<BasicDigraph.Edge>(graph);
			ArrayList<ArrayDeque<Integer>> paths = euler.getPaths();
			
			for (ArrayDeque<Integer> path : paths) {			
				if (addedSinkSourceEdge)
					path.pollLast();
				
				String seq = new String();
				for (int i : path) {
					if (seq.length() == 0) {
						if (merStrings[i] != null) 
							seq = merStrings[i].toDisplayString();
						else
							seq = Mer.fromInt(mers[i], vertexMerLength);
					}
					else {
						if (merStrings[i] != null)
							seq += merStrings[i].toDisplayString().substring(vertexMerLength - 1);
						else
							seq += Mer.fromInt(mers[i], 1);
					}
				}
				
				result.add(seq);
			}
			
			return result;			
		}
		
		private int[] mers;
		private MerString[] merStrings;
		private BasicDigraph graph;
		private boolean addedSinkSourceEdge;
	}

	private int vertexMerLength;
	private int coverage;
	
}
