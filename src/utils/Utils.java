package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.antlr.grammar.v3.CodeGenTreeWalker.element_action_return;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public class Utils {
	public static void getB(String url, int k, int l, double[][] B, int bXCol,
			int bYCol) throws IOException {

		Path pt = new Path(url);
		FileSystem fs = FileSystem.get(new Configuration());
		List<Path> realFileNames = getFileIterator(pt, fs);
		for (Path currPath : realFileNames) {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(currPath)));
			String line = null;
			line = br.readLine();
			int userClusterCount = -1;
			int itemClusterCount = -1;
			while (line != null) {
				String[] split = line.split("\t");
				userClusterCount = Integer.parseInt(split[bXCol]);
				itemClusterCount = Integer.parseInt(split[bYCol]);
				B[userClusterCount][itemClusterCount] = Double
						.parseDouble(split[2]);

				line = br.readLine();
			}
		}
	}

	public static String initializeAlphas(String prefix, int n)
			throws IOException {
		Path pt = new Path(prefix + "alphas_0");
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream file = fs.create(pt);
		double initialValue = 1.0 / n;
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(file));
		for (int i = 0; i < n; i++) {
			bw.write(i + "\t" + initialValue + "\n");
		}
		bw.close();
		file.close();
		fs.close();
		return pt.toString();
	}

	public static void initializeV(String prefix, String dataPath,
			char seperator, int vColIndex, String initialSchema, int L, int N,
			String vOutputPath) throws Exception {
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		String[] cols = initialSchema.split(",");
		String vColName = cols[vColIndex].split(":")[0];
		// pigServer.setDefaultParallel(k);
		pigServer.registerQuery("data = LOAD '" + prefix + dataPath
				+ "' USING PigStorage('" + seperator + "') AS ("
				+ initialSchema + ");");
		pigServer.registerQuery("V = group data by " + vColName + ";");

		StringBuilder randomClusterQuery = new StringBuilder(
				"V = foreach V generate group as iid");

		String randomValueGenCol = String.format(
				",(INT)(RANDOM() * %d) as itemSolId", (int) Math.pow(L, N));
		randomClusterQuery.append(randomValueGenCol);
		randomClusterQuery.append(";");
		pigServer.registerQuery(randomClusterQuery.toString());

		pigServer.store("V", prefix + vOutputPath);

		pigServer.shutdown();
	}

	public static double[] loadAlphas(String alphasUrl, int n)
			throws IOException {
		double[] result = new double[n];
		Path pt = new Path(alphasUrl);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		String line = null;
		line = br.readLine();
		String[] coordinatesSplit = null;
		int userClusterCount = -1;
		int itemClusterCount = -1;
		while (line != null) {
			String[] split = line.split("\t");
			int sourceId = Integer.parseInt(split[0]);
			double alpha = Double.parseDouble(split[1]);
			result[sourceId] = alpha;
			line = br.readLine();
		}
		return result;
	}

	public static String generateFullJoin(int t, String dataPath,
			String initialSchema, char seperator, String prefix,
			boolean isUsersJoin) throws IOException {
		String tName = "fullJoin_";
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		pigServer.registerQuery("data = LOAD '" + prefix + dataPath
				+ "' USING PigStorage('" + seperator + "') AS ("
				+ initialSchema + ");");

		if (isUsersJoin) {
			pigServer
			.registerQuery("T = LOAD '"
					+ prefix
					+ "V_"
					+ (t - 1)
					+ "' USING PigStorage('" + seperator + "') AS (xid:int, xSolNumber:int)  ;");
			pigServer.registerQuery("fullJoin = JOIN T BY xid, data BY iid;");
		} else {
			pigServer
			.registerQuery("T = LOAD '"
					+ prefix
					+ "U_"
					+ (t)
					+ "' USING PigStorage('\t') AS (xid:int, xSolNumber:int)  ;");
			pigServer.registerQuery("fullJoin = JOIN T BY xid, data BY uid;");
			tName += "V_";
		}
		pigServer
		.registerQuery("fullJoin = foreach fullJoin GENERATE data::uid AS uid, data::iid AS iid , T::xSolNumber AS xSolNumber, data::rating AS rating;");

		String outputPath = tName + t;

		pigServer.store("fullJoin", prefix + outputPath);
		pigServer.shutdown();
		return outputPath;
	}

	public static String generateFullJoinForPreprocessForAlphasLearning(int t,
			String dataPath, String dataInitialSchema, char seperator,
			String prefix, boolean isForRMSE) throws IOException {
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		pigServer.registerQuery("data = LOAD '" + prefix + dataPath
				+ "' USING PigStorage('" + seperator + "') AS ("
				+ dataInitialSchema + ");");
		pigServer.registerQuery("V = LOAD '" + prefix + "V_" + (t)
				+ "' USING PigStorage('\t') AS (iid:int, iSolNumber:int)  ;");
		pigServer.registerQuery("U = LOAD '" + prefix + "U_" + (t)
				+ "' USING PigStorage('\t') AS (uid:int, uSolNumber:int)  ;");
		pigServer.registerQuery("dataJoinV = JOIN data BY iid, V BY iid;");
		pigServer
		.registerQuery("dataJoinVJoinU = JOIN dataJoinV BY uid, U BY uid;");
		pigServer
		.registerQuery("fullJoin = foreach dataJoinVJoinU GENERATE data::uid AS uid, data::iid AS iid, U::uSolNumber AS uSolNumber , V::iSolNumber AS iSolNumber, data::rating as rating;");
		String outputPath = null;
		if (!isForRMSE){
			outputPath = "AlphasLearningFullJoin_" + t;
		}else {
			outputPath = "RMSEFullJoin_" + t;
		}
		pigServer.store("fullJoin", prefix + outputPath);
		pigServer.shutdown();
		return prefix + outputPath;
	}

	public static RealMatrix inversMatrix(double[][] matrixData)
			throws IOException {
		RealMatrix m = MatrixUtils.createRealMatrix(matrixData);
		RealMatrix pInverse = new LUDecomposition(m).getSolver().getInverse();
		//		RealMatrix pInverse = new QRDecomposition(m).getSolver().getInverse();
		return pInverse;
	}

	public static double[][] getMatrixFromFile(String matrixPath, int rows,
			int coloumns) throws IOException, FileNotFoundException {
		double[][] matrixData = new double[rows][coloumns];

		Path pt = new Path(matrixPath);
		FileSystem fs = FileSystem.get(new Configuration());
		List<Path> realFileNames = getFileIterator(pt, fs);

		for (Path currPath : realFileNames) {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(currPath)));
			String line = null;
			line = br.readLine();
			while (line != null) {
				String[] split = line.split("\t");
				int x = Integer.parseInt(split[0]);
				int y = 0;
				double val = 0;
				if (coloumns ==1){
					//					y = 0;
					val = Double.parseDouble(split[1]);
				}else{
					y = Integer.parseInt(split[1]);
					val = Double.parseDouble(split[2]);
				}

				matrixData[x][y] = val;
				line = br.readLine();
			}
		}
		return matrixData;
	}

	private static List<Path> getFileIterator(Path pt, FileSystem fs)
			throws IOException, FileNotFoundException {
		List<Path> realFileNames = new ArrayList<Path>();
		if (!fs.isDirectory(pt)) {
			realFileNames.add(pt);

		} else {
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(pt,
					false);
			while (listFiles.hasNext()) {
				Path path = listFiles.next().getPath();
				if (path.getName().contains("part")) {
					realFileNames.add(path);
				}

			}
		}
		return realFileNames;
	}

	public static String saveMatrixToFile(String prefix, String fileName,
			double[][] multiply) throws IOException {
		Path pt = new Path(prefix + fileName);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream file = fs.create(pt);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(file));
		for (int i = 0; i < multiply.length; i++) {
			bw.write(i + "\t" + multiply[i][0] + "\n");
		}
		bw.close();
		file.close();
		fs.close();
		return pt.toString();
	}
	public static void printMatrix(double[][] leftMatrix) {
		for (double[] ds : leftMatrix) {
			System.out.println(Arrays.toString(ds));
		}
	}

	public static double readRMSEFromFile(String output) throws IOException {
		Path pt = new Path(output);
		FileSystem fs = FileSystem.get(new Configuration());
		List<Path> realFileNames = getFileIterator(pt, fs);
		for (Path currPath : realFileNames) {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(currPath)));
			String line = null;
			line = br.readLine();
			if (line != null) {
				String[] split = line.split("\t");
				return Double.parseDouble(split[1]);
			
			}
		}
		return Double.NaN;
	}
}
