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

import java.io.BufferedReader;
import java.io.IOException; 
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

// A sample driver application for running the MRAssembler class with Hadoop.
// The test data in this case is a simple set of short reads, with no errors
// or repetitive sections, yielding a final sequence that is pretty short.

public class MRAssemblerTest1 {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		setupTest(conf);
		
		MRAssembler assembler = new MRAssembler(MER_LENGTH, COVERAGE);
		assembler.run(new Path(testInput), new Path(testOutput));
		
		verifyTest(conf);
		cleanupTest(conf);
		
		System.exit(0);
	}
	
	private static void setupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path path = new Path(testInput);
		if (fileSystem.exists(path))
			fileSystem.delete(path, true);
		
		ArrayList<Text> reads = new ArrayList<Text>();
		
		// Goal: AATTCGGCCTTCGGCAT

		reads.add(new Text("AATTCGGC\n"));
		reads.add(new Text("CTTCGGCAT\n"));
		
		reads.add(new Text("AATT\n"));
		reads.add(new Text("CGGCCTTCGGCAT\n"));
		
		reads.add(new Text("AATTCGGCCTTCG\n"));
		reads.add(new Text("GCAT\n"));

		FSDataOutputStream out = fileSystem.create(path);
		for (Text read : reads) {
			byte[] bytes = read.copyBytes();
			for (byte b : bytes)
				out.write(b);
		}
		out.close();
		
		fileSystem.close();
	}
	
	private static void verifyTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataInputStream output = fileSystem.open(new Path(testOutput));
		BufferedReader reader = new BufferedReader(new InputStreamReader(output));

		String actual = reader.readLine();
		final String expected = "AATTCGGCCTTCGGCAT";
		
		System.out.println(expected);
		
		if (!actual.equals(expected))
			throw new IOException("Test failed with incorrect result: " + actual);
		
		reader.close();
		
		System.out.println("Test succeeded.");
	}

	private static void cleanupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		fileSystem.delete(new Path(testInput), true);
		fileSystem.delete(new Path(testOutput), true);
		
		fileSystem.close();
	}
	
	private static final int MER_LENGTH = 3;
	private static final int COVERAGE = 3;

	private static String testInput = new String("MRAssemblerTest_in.txt");
	private static String testOutput = new String("MRAssemblerTest_out");
}
