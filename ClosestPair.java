import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ClosestPair {

	public static class Point {
		public final double x;
		public final double y;
		public final int index;

		public Point(int index, double x, double y) {
			this.index = index;
			this.x = x;
			this.y = y;
		}

	}

	public static class Pair {
		public Point point1 = null;
		public Point point2 = null;
		public double distance = 0.0;

		public Pair() {
			// empty
		}

		public Pair(Point point1, Point point2) {
			this.point1 = point1;
			this.point2 = point2;
			find_distance();
		}

		public void update(Point point1, Point point2, double distance) {
			this.point1 = point1;
			this.point2 = point2;
			this.distance = distance;
		}

		public void find_distance() {
			this.distance = cal_distance(point1, point2);
		}

		public String toString() {
			return point1.index + "\n" + point2.index;
		}

	}

	public static double cal_distance(Point p1, Point p2) {
		double xdis = p2.x - p1.x;
		double ydis = p2.y - p1.y;
		return Math.hypot(xdis, ydis);
	}

	public static Pair BruteForce(List<Point> points) {
		int point_num = points.size();
		if (point_num < 2)
			return null;
		Pair pair = new Pair(points.get(0), points.get(1));
		for (int i = 0; i < point_num - 1; i++) {
			Point p1 = points.get(i);
			for (int j = i + 1; j < point_num; j++) {
				Point p2 = points.get(j);
				double distance = cal_distance(p1, p2);

				if (distance < pair.distance) {
					pair.update(p1, p2, distance);
				}
			}
		}
		return pair;
	}

	public static void sortX(List<Point> points) {
		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point p1, Point p2) {
				if (p1.x < p2.x)
					return -1;
				else if (p1.x > p2.x)
					return 1;
				return 0;
			}
		});
	}

	public static void sortY(List<Point> points) {
		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point p1, Point p2) {
				if (p1.y < p2.y)
					return -1;
				else if (p1.y > p2.y)
					return 1;
				return 0;
			}
		});
	}

	public static Pair Divide_Conquer(List<Point> points) {
		List<Point> xP = new ArrayList<Point>(points);
		sortX(xP);
		List<Point> yP = new ArrayList<Point>(points);
		sortY(yP);
		return Divide_Conquer(xP, yP);
	}

	private static Pair Divide_Conquer(List<Point> xP, List<Point> yP) {
		int N = xP.size();

		if (N <= 3)
			return BruteForce(xP);
		int divide_index = N / 2;

		List<Point> xL = xP.subList(0, divide_index);
		List<Point> xR = xP.subList(divide_index+1, N);
		double xm = xR.get(0).x;

		List<Point> yL = new ArrayList<Point>(xL);
		sortY(yL);
		List<Point> yR = new ArrayList<Point>(xR);
		sortY(yR);
		Pair pairL = Divide_Conquer(xL, yL);
		Pair pairR = Divide_Conquer(xR, yR);

		Pair closestPair = new Pair();

		if (pairR.distance < pairL.distance)
			closestPair = pairR;
		else if (pairR.distance > pairL.distance)
			closestPair = pairL;

		double dmin = closestPair.distance;
		List<Point> yS = new ArrayList<Point>();
		for (Point point : yP) {
			if (Math.abs(xm - point.x) < dmin)
				yS.add(point);
		}
		int nS = yS.size();

		for (int i = 0; i < nS - 1; i++) {
			int k = i + 1;
			Point S_k = yS.get(i);
			Point S_i = yS.get(k);
			while ((k <= nS) && (S_k.y - S_i.y) < dmin) {
				double distance = cal_distance(S_k, S_i);
				if (distance < dmin)
					closestPair.update(S_k, S_i, distance);
				k += 1;
			}
		}

		return closestPair;
	}

	static void ReadTxt(String path, List<Point> points) throws IOException {
		try {
			BufferedReader in = new BufferedReader(new FileReader(path));
			String s;
			while ((s = in.readLine()) != null) {
				String[] line = s.split(",");
				if (line.length < 2)
					continue;
				int index = Integer.parseInt(line[0]);
				double x = Double.parseDouble(line[1]);
				double y = Double.parseDouble(line[2]);
				points.add(new Point(index, x, y));
			}
			in.close();
		} catch (IOException e) {
			System.err.println(e);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws IOException {
		List<Point> point = new ArrayList<Point>();
		ReadTxt("n100d2.txt", point);

		Pair BruteForce = BruteForce(point);
		System.out.println(BruteForce);
		Pair Divide_Conquer = Divide_Conquer(point);
		System.out.println(Divide_Conquer);
	}
}
