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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class MRAssemblerTest {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		setupTest(conf);
		
		MRAssembler assembler = new MRAssembler(MER_LENGTH);
		assembler.run(new Path(testInput), new Path(testOutput));
		
		cleanupTest(conf);
		
		System.exit(0);
	}
	
	private static void setupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path path = new Path(testInput);
		if (fileSystem.exists(path))
			fileSystem.delete(path, true);
		
		ArrayList<Text> reads = new ArrayList<Text>();
		reads.add(new Text("ACGT\n"));
		reads.add(new Text("CGTA\n"));
		reads.add(new Text("GTAC\n"));
		reads.add(new Text("TACA\n")); // branch, start of repeated section
		reads.add(new Text("ACAG\n"));
		reads.add(new Text("CAGT\n"));
		reads.add(new Text("AGTC\n"));
		reads.add(new Text("GTCG\n")); // branch, end of repeated section
		reads.add(new Text("TCGA\n"));
		reads.add(new Text("CGAG\n"));
		reads.add(new Text("GAGC\n"));
		reads.add(new Text("AGCC\n"));
		reads.add(new Text("GCCT\n"));
		reads.add(new Text("CCTC\n"));
		reads.add(new Text("CTCT\n"));
		reads.add(new Text("TCTA\n"));
		reads.add(new Text("CTAC\n"));
		reads.add(new Text("TACA\n")); // branch, start of repeated section, again
		reads.add(new Text("ACAG\n"));
		reads.add(new Text("CAGT\n"));
		reads.add(new Text("AGTC\n"));
		reads.add(new Text("GTCA\n")); // branch, end of repeated section, again
		reads.add(new Text("TCAT\n"));
		reads.add(new Text("CATG\n"));

		FSDataOutputStream out = fileSystem.create(path);
		for (Text read : reads) {
			byte[] bytes = read.copyBytes();
			for (byte b : bytes)
				out.write(b);
		}
		out.close();
		
		fileSystem.close();
	}
	
	private static void cleanupTest(Configuration conf) throws IOException {
		System.out.println("ACGTACAGTCGAGCCTCTACAGTCATG");
		
		FileSystem fileSystem = FileSystem.get(conf);
		
		fileSystem.delete(new Path(testInput), true);
		// HEY!!
		//fileSystem.delete(new Path(testOutput), true);
		
		fileSystem.close();
	}
	
	private static final int MER_LENGTH = 3;
	private static String testInput = new String("MRAssemblerTest_in.txt");
	private static String testOutput = new String("MRAssemblerTest_out");
}