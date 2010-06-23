package edu.usu.cosl.recommender.personal;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.common.parameters.Parameter;
import org.apache.mahout.math.CardinalityException;
import org.apache.mahout.math.Vector;
import org.apache.mahout.common.distance.DistanceMeasure;

/**
 * This class implements a "manhattan distance" metric by summing the absolute values of the difference
 * between each coordinate
 */
public class MyDistanceMeasure implements DistanceMeasure {
  public static double maxDistance = 0;
  public static double minDistance = 100000;
  public static double avgDistance = 0;
  public static double sumDistances = 0;
  public static int nPoints = 0;
	
  public static double distance(double[] p1, double[] p2) {
    double result = 0.0;
    for (int i = 0; i < p1.length; i++) {
      result += Math.abs(p2[i] - p1[i]);
    }
    if (result > maxDistance) maxDistance = result;
    if (result < minDistance) minDistance = result;
    return result;
  }
  
  public void configure(JobConf job) {
  // nothing to do
  }
  
  public Collection<Parameter<?>> getParameters() {
    return Collections.emptyList();
  }
  
  public void createParameters(String prefix, JobConf jobConf) {
  // nothing to do
  }
  
  public double distance(Vector v1, Vector v2) {
    if (v1.size() != v2.size()) {
      throw new CardinalityException();
    }
    double result = 0;
    Vector vector = v1.minus(v2);
    Iterator<Vector.Element> iter = vector.iterateNonZero(); 
    // this contains all non zero elements between the two
    while (iter.hasNext()) {
      Vector.Element e = iter.next();
      result += Math.abs(e.get());
    }
    if (result > maxDistance) maxDistance = result;
    if (result < minDistance) minDistance = result;
    sumDistances += result;
    nPoints++;
    avgDistance = sumDistances/nPoints;
    return result;
  }
  
  public double distance(double centroidLengthSquare, Vector centroid, Vector v) {
    return distance(centroid, v); // TODO
  }
  
}
