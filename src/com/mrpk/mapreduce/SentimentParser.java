package com.mrpk.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

public class SentimentParser {
	static Map<String, Integer> dictpos;
	static Map<String, Integer> dictneg;
	public SentimentParser() {
		dictpos = new HashMap<String, Integer>();
		dictneg = new HashMap<String, Integer>();
	}
	public boolean initpos() {
		try {
			BufferedReader bReader = new BufferedReader(new FileReader("positive.txt"));
			String line = null;
			while ((line = bReader.readLine()) != null) {
				dictpos.put(line, 1);
			}
			bReader.close();
			return true;
		} catch(Exception e) {
			return false;
		}
	}
	public boolean initnega() {
		try {
			BufferedReader bReader = new BufferedReader(new FileReader("negative.txt"));
			String line = null;
			while ((line = bReader.readLine()) != null) {
				dictneg.put(line, 1);
			}
			bReader.close();
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}
	}
	public int countScore(Text text) {
		int score = 0;
		String line = text.toString();
		if (line == null)
			return score;
		StringTokenizer tokenizer = new StringTokenizer(line);
		Text word = new Text();
		int count = 0;
		while (tokenizer.hasMoreTokens()) {
			if (count < 2) {
				count++;
				tokenizer.nextToken();
			}
			word.set(tokenizer.nextToken());
			if (dictpos.containsKey(word.toString())) {
				score += 1;
			} else if (dictneg.containsKey(word.toString())) {
				score -= 1;
			}
		}
		return score;
	}
}
