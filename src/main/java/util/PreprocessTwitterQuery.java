package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import io.StreamUtils;

public class PreprocessTwitterQuery {
	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
		OutputStream output = StreamUtils.outputStream(args[1]);
		String line = null;
		while (null != (line = reader.readLine())) {
			String[] fields = line.split("\t");
			List<Integer> queryTime = new ArrayList<Integer>();
			for (int i = 3; i < fields.length; i++) {
				queryTime.add((int) (Long.parseLong(fields[i]) / (1000 * 60 * 30)));
			}
			output.write(
					String.format("%s\t%s\t%s\n", fields[0], fields[1], StringUtils.join(queryTime, "\t")).getBytes());
		}
		output.close();
		reader.close();
	}
}
