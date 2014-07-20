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
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.philiphubbard.digraph.MRVertex;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

public class MRMerVertex extends MRVertex {
	
	public static void setMerLength(int length) {
		merLength = length;
	}
	
	public static int getMerLength() {
		return merLength;
	}
	
	//
	
	public MRMerVertex(int id) {
		super(id);
		merString = null; // HEY!! new MerString(id, merLength);
	}
	
	public MRMerVertex(Text text) {
		this(text.toString());
	}
	
	public MRMerVertex(String string) {
		super(string);
	}
	
	public MerString getMerString() {
		return merString;
	}
	
	public boolean equals(MRMerVertex other) {
		if (!super.equals(other))
			return false;
		return (merString.equals(other.merString));
	}
	
	public String toDisplayString() {
		StringBuilder s = new StringBuilder();
		
		s.append("MRMerVertex ");
		s.append(getId());
		
		s.append(" (");
		s.append(Mer.fromInt(getId(), getMerLength()));
		s.append(") ");
		
		MRVertex.AdjacencyIterator toIt = createToAdjacencyIterator();
		if (toIt.begin() != NO_VERTEX) {
			s.append("; to: ");
			for (int to = toIt.begin(); !toIt.done(); to = toIt.next()) {
				s.append(to);
				s.append(" ");
			}
		}
		
		MRVertex.AdjacencyIterator fromIt = createFromAdjacencyIterator();
		if (fromIt.begin() != NO_VERTEX) {
			s.append("; from: ");
			for (int from = fromIt.begin(); !fromIt.done(); from = fromIt.next()) {
				s.append(from);
				s.append(" ");
			}
		}
		
		if (merString != null) {
			s.append("; mer ");
			s.append(merString.toDisplayString());
		}
		
		return s.toString();
	}

	// HEY!! How to make a general version of this instead of duplicating it?
	public static void read(FSDataInputStream in, ArrayList<MRVertex> vertices) throws IOException {
		InputStreamReader inReader = new InputStreamReader(in, Charset.forName("UTF-8"));
		BufferedReader bufferedReader = new BufferedReader(inReader);
		String line;
		while((line = bufferedReader.readLine()) != null) {
			String[] tokens = line.split("\t", 2);
			MRVertex vertex = new MRMerVertex(tokens[1]);
			vertices.add(vertex);
		}
	}
	

	//
	
	protected void mergeInternal(MRVertex other) {
		if (other instanceof MRMerVertex) {
			MRMerVertex otherMer = (MRMerVertex) other;

			if (merString == null)
				merString = new MerString(getId(), merLength);
			MerString otherMerString = otherMer.merString;
			if (otherMerString == null)
				otherMerString = new MerString(other.getId(), merLength);
				
			// HEY!! 
			System.out.println("*** " + merString.toDisplayString() + " + " + otherMerString.toDisplayString() + 
					" [" + (merLength - 1) + "] ***");
			
			merString.merge(otherMerString, merLength - 1);
			
			// HEY!! 
			System.out.println("**** " + merString.toDisplayString() + " ****");
		}
	}
	
	protected String toTextInternal() {
		/* HEY!!
		if (merString != null) {
			String s0 = new String("*");
			String s1 = s0 + merString.toString();
			System.out.print("Test string concat: ");
			for (char c : s1.toCharArray())	
				System.out.printf("%02x ", (int) c);
		}
		*/
			
		if (merString != null)
			return merString.toString();
		else
			return null;
	}
	
	protected void fromTextInternal(String s) {
		merString = new MerString(s);
	}
	
	//
	
	private static int merLength;
	private MerString merString;

}
