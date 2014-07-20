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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.philiphubbard.digraph.MRCompressChains;
import com.philiphubbard.digraph.MRVertex;

public class MRCompressMerChains extends MRCompressChains {
	
	public static void setupIterationJob(Job job, Path inputPathOrig, Path outputPathOrig)
			throws IOException {
		MRCompressChains.setupIterationJob(job, inputPathOrig, outputPathOrig);
		
		job.setMapperClass(MRCompressMerChains.Mapper.class);
		job.setCombinerClass(MRCompressMerChains.Reducer.class);
		job.setReducerClass(MRCompressMerChains.Reducer.class);
	}

	public static class Mapper extends MRCompressChains.Mapper {

		protected MRVertex createMRVertex(Text value) {
			
			// HEY!! Debug
			MRMerVertex vertex = new MRMerVertex(value); 
			System.out.println("** createMRVertex(); created " + vertex.toDisplayString() + " **");
			return vertex;

			// HEY!! Orig
			//return new MRMerVertex(value);
		}
		
	}

	public static class Reducer extends MRCompressChains.Reducer {

		protected MRVertex createMRVertex(Text value) {
			
			// HEY!! Debug
			MRMerVertex vertex = new MRMerVertex(value); 
			System.out.println("** createMRVertex(); created " + vertex.toDisplayString() + " **");
			return vertex;

			// HEY!! Orig
			//return new MRMerVertex(value);
		}

	}
	
}
