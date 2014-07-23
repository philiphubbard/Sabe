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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;

import com.philiphubbard.digraph.MRCompressChains;
import com.philiphubbard.digraph.MRVertex;
import com.philiphubbard.sabe.MRBuildMerVertices;

public class MRAssembler {

	// HEY!! Should the entry point be static?
	// HEY!! More specific about exceptions thrown?  Make that change elsewhere?
	public static boolean run(Path inputPath, Path outputPath, int merLength) 
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
		MRBuildMerVertices.setVertexMerLength(merLength);
		
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
		
		buildCompressedGraph(conf, fileSystem, branchPath, chainPath);
		
		//
		
		fileSystem.delete(buildOutputPath, true);
		
		fileSystem.close();	

		return true;
	}
	
	protected static void buildCompressedGraph(Configuration conf, FileSystem fileSystem, 
			Path branchPath, Path chainPath) 
			throws IOException, InterruptedException {
		ArrayList<MRVertex> vertices = new ArrayList<MRVertex>();
		
		FileStatus[] branchFiles = fileSystem.listStatus(branchPath);
		for (FileStatus status : branchFiles) 
			readVertices(status, vertices, conf);
		
		FileStatus[] chainFiles = fileSystem.listStatus(chainPath);
		for (FileStatus status : chainFiles)
			readVertices(status, vertices, conf);
		
		/// HEY!!
		for (MRVertex vertex : vertices) 
			System.out.println(vertex.toDisplayString());
	}

	private static void readVertices(FileStatus status, ArrayList<MRVertex> vertices, Configuration conf)
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
	
}
