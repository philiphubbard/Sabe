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

// HEY!! This order of imports is what Eclipse generated.  So switch to it everywhere?
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
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
import com.philiphubbard.digraph.MRCompressChains;
import com.philiphubbard.digraph.MRVertex;
import com.philiphubbard.sabe.MRBuildMerVertices;

public class MRAssembler {

	public MRAssembler(int vertexMerLength) {
		this.vertexMerLength = vertexMerLength;
	}
	
	// HEY!! More specific about exceptions thrown?  Make that change elsewhere?
	public boolean run(Path inputPath, Path outputPath) 
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		//
		
		Job buildJob = Job.getInstance(conf);
		buildJob.setJobName("mrassemblerbuild");
		
		Path buildInputPath = inputPath;
		Path buildOutputPath = new Path("MRAssemblerTmp");
		
		/* HEY!!
		System.out.print("inputPath \"" + inputPath.toString() + "\"\n");
		System.out.print("outputPath \"" + outputPath.toString() + "\"\n");
		System.out.print("outputDir \"" + outputDir.toString() + "\"\n");
		System.out.print("buildOutputPath \"" + buildOutputPath.toString() + "\"\n");
		return true;
		*/

		MRBuildMerVertices.setupJob(buildJob, buildInputPath, buildOutputPath);	
		MRBuildMerVertices.setPartitionBranchesChains(true);
		MRBuildMerVertices.setVertexMerLength(vertexMerLength);
		
		if (!buildJob.waitForCompletion(true))
			return false;
		
		//
		
		// HEY!! Need MRCompressMerChains which extends MRCompressChains and overrides
		// createMRVertex(Text value) to return MRMerVertex.

		Path compressInputPath = new Path(buildOutputPath.toString() + "/chain");
		Path compressOutputPath = new Path(buildOutputPath.toString() + "/chainCompress");
		
		int iter = 0;
		boolean keepGoing = true;
		MRCompressChains.beginIteration();
		while (keepGoing) {
			Job compressJob = Job.getInstance(conf);
			compressJob.setJobName("mrassemblercompress");

			MRCompressMerChains.setupIterationJob(compressJob, compressInputPath, compressOutputPath);
			
			if (!compressJob.waitForCompletion(true))
				System.exit(1);
			
			iter++;
			keepGoing = MRCompressChains.continueIteration(compressJob, compressInputPath, compressOutputPath);
		}
		
		//
		
		Path branchPath = new Path(buildOutputPath.toString() + "/branch");
		Path chainPath = compressOutputPath;
		
		FileSystem fileSystem = FileSystem.get(conf);
		
		Graph graph = buildCompressedGraph(conf, fileSystem, branchPath, chainPath);
		ArrayList<String> result = graph.assemble();
		
		// HEY!!
		String seq = result.get(0);
		System.out.println(seq);
		
		//
		
		fileSystem.delete(buildOutputPath, true);
		
		fileSystem.close();	

		return true;
	}
	
	protected Graph buildCompressedGraph(Configuration conf, FileSystem fileSystem, 
			Path branchPath, Path chainPath) 
			throws IOException, InterruptedException {
		ArrayList<MRMerVertex> vertices = new ArrayList<MRMerVertex>();
		
		FileStatus[] branchFiles = fileSystem.listStatus(branchPath);
		for (FileStatus status : branchFiles) 
			readVertices(status, vertices, conf);
		
		FileStatus[] chainFiles = fileSystem.listStatus(chainPath);
		for (FileStatus status : chainFiles)
			readVertices(status, vertices, conf);
		
		/// HEY!!
		for (MRVertex vertex : vertices) 
			System.out.println(vertex.toDisplayString());
		
		return new Graph(vertices);
	}

	private void readVertices(FileStatus status, ArrayList<MRMerVertex> vertices, Configuration conf)
			throws IOException {
		Path path = status.getPath();
		if (path.getName().startsWith("part")) {
			System.out.println(path); 
			
		    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		    IntWritable key = new IntWritable();
		    BytesWritable value = new BytesWritable();
		    while (reader.next(key, value))
		    	vertices.add(new MRMerVertex(value));
		    reader.close();
		}
	}
	
	protected class Graph {
		public Graph(ArrayList<MRMerVertex> vertices) {
			mers = new int[vertices.size()];
			merStrings = new MerString[vertices.size()];
			merToIndex = new HashMap<Integer, Integer>(vertices.size());
			
			MRMerVertex source = null;
			MRMerVertex sink = null;

			int i = 1;
			for (MRMerVertex vertex : vertices) {
				if (vertex.getIsSource()) {
					// HEY!!
					if (source == null)
						System.out.println("** " + i + " is source **");
					
					if (source == null)
						source = vertex;
					// HEY!! Else what?
				}
				
				if (vertex.getIsSink()) {
					// HEY!!
					if (sink == null)
						System.out.println("** " + i + " is sink **");
					
					if (sink == null)
						sink = vertex;
					// HEY!! Else what?
				}

				int j = (vertex == source) ? 0 : i++;
				mers[j] = vertex.getId();
				merStrings[j] = vertex.getMerString();
				merToIndex.put(mers[j], j);
				
				// HEY!!
				if (merStrings[j] != null)
					System.out.println("* " + j + ": mers " + mers[j] 
							+ " merStrings " + merStrings[j].toDisplayString() + " *");
				else
					System.out.println("* " + j + ": mers " + mers[j] 
							+ " no merStrings *");
			}
			
			graph = new BasicDigraph(vertices.size(), Digraph.EdgeMultiples.ENABLED);
			
			for (MRMerVertex vertex : vertices) {
				int j = merToIndex.get(vertex.getId());
				System.out.print("** " + j + ": ");
				
				MRVertex.AdjacencyIterator it = vertex.createToAdjacencyIterator();
				for (int to = it.begin(); !it.done(); to = it.next()) {
					BasicDigraph.Edge edge = new BasicDigraph.Edge(merToIndex.get(to).intValue());
					graph.addEdge(j, edge);
					
					// HEY!!
					System.out.print(edge.getTo() + " ");
				}
				
				// HEY!!
				System.out.print("\n");
			}
			
			addedSinkSourceEdge = false;
			if ((source != null) && (sink != null)) {
				int from = merToIndex.get(sink.getId()).intValue();
				int to = merToIndex.get(source.getId()).intValue();
				graph.addEdge(from, new BasicDigraph.Edge(to));
				addedSinkSourceEdge = true;
			}
		}
		
		public ArrayList<String> assemble() {
			ArrayList<String> result = new ArrayList<String>();
			
			EulerPaths<BasicDigraph.Edge> euler = new EulerPaths<BasicDigraph.Edge>(graph);
			ArrayList<ArrayDeque<Integer>> paths = euler.getPaths();
			
			for (ArrayDeque<Integer> path : paths) {			
				if (addedSinkSourceEdge)
					path.pollLast();
				
				// HEY!!
				for (int v : path)
					System.out.print(v + " ");
				System.out.print("\n");
				
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
		private HashMap<Integer, Integer> merToIndex;
		private BasicDigraph graph;
		private boolean addedSinkSourceEdge;
	}

	private int vertexMerLength;
	
}
