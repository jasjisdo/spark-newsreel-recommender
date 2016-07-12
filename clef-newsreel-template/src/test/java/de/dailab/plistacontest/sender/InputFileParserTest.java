package de.dailab.plistacontest.sender;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.mahout.math.Arrays;
import org.junit.Ignore;

import eu.crowdrec.contest.sender.LogFileUtils;

import junit.framework.TestCase;

/**
 * Test the input file parser
 * @author andreas
 *
 */

public class InputFileParserTest {

	@Ignore
	public void testParseClefLogfile() {
		String fileName = "CLEF-line.txt";
		try {
			BufferedReader bw = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = bw.readLine();
			String[] tmp = LogFileUtils.extractEvaluationRelevantDataFromInputLine(line);
			String outputString = Arrays.toString(tmp);
			System.out.println(outputString);
			final String expected = "[1383778799938, 3102198867, 147354771, 596, 1383778799938]";
			TestCase.assertEquals(expected, outputString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Ignore
	public void testParseIdomaarLogfile() {
		String fileName = "CLEF-line.txt.idomaar.txt";
		try {
			BufferedReader bw = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = bw.readLine();
			String[] tmp = LogFileUtils.extractEvaluationRelevantDataFromInputLine(line);
			String outputString = Arrays.toString(tmp);
			System.out.println(outputString);
			final String expected = "[9055857000596, 3102198867, 147354771, 596, 1383778799938]";
			TestCase.assertEquals(expected, outputString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
