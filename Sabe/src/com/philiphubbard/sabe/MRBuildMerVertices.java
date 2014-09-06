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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.philiphubbard.digraph.MRBuildVertices;
import com.philiphubbard.digraph.MRVertex;

// A class derived from digraph.MRBuildVertices, specializing that class's
// mapper to build instances of the MRMerVertex class, derived from MRVertex.
// The input to the mapper is a set of "read" strings.

public class MRBuildMerVertices extends MRBuildVertices {
	
	public static final String CONFIG_VERTEX_MER_LENGTH = "CONFIG_VERTEX_MER_LENGTH";
	
	public static void setupJob(Job job, Path inputPath, Path outputPath) 
			throws IOException {
		MRBuildVertices.setupJob(job, inputPath, outputPath);
	
		job.setMapperClass(MRBuildMerVertices.Mapper.class);
	}
	
	// The mapper simply overrides the verticesFromInputValue() function of the
	// MRCollectVertices.Mapper class, to take input in the form of "read" strings.
	// Each read should be a line of the input file, ending with "\n".
	
	public static class Mapper extends MRBuildVertices.Mapper {
		
		@Override
		protected ArrayList<MRVertex> verticesFromInputValue(Text value, Configuration config) {
			ArrayList<MRVertex> result = new ArrayList<MRVertex>();
			
			int vertexMerLength = config.getInt(MRMerVertex.CONFIG_MER_LENGTH, 1);
			
			String s = value.toString();
			MRMerVertex prev = null;
			for (int i = 0; i < s.length() - vertexMerLength + 1; i++) {
				String mer = s.substring(i, i + vertexMerLength);
				
				int id = Mer.toInt(mer);
				MRMerVertex curr = new MRMerVertex(id, config);
				result.add(curr);
				
				if (prev != null)
					prev.addEdgeTo(id);
				prev = curr;
			}
			
			return result;
		}

	}
	
	// The MRCollectVertexEdges.Reducer class can be used as is.
	
}
