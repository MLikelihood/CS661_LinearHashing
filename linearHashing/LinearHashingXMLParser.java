package SZ_LH.linearHashing;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public class LinearHashingXMLParser {

	private int M, sp, pageCapacity,totalPages;
	private double lambdaAverage, lambdaUpper, lambdaLower;
	private String LHName;
	private ArrayList<Integer> Directory = new ArrayList<Integer>();

	public ArrayList<Integer> getDirectory() {
		return this.Directory;
	}

	public int getPageCapacity() {
		return this.pageCapacity;
	}

	public int getM() {
		return this.M;
	}

	public int getSP() {
		return this.sp;
	}

	public double getLambdaAverage() {
		return this.lambdaAverage;
	}

	public double getLambdaUpper() {
		return this.lambdaUpper;
	}

	public double getLambdaLower() {
		return this.lambdaLower;
	}

	public String getName() {
		return this.LHName;
	}
	
	public int getTotalPages(){
		return this.totalPages;
	}

	public void setTotalPages(int totalPages){
		this.totalPages=totalPages;
	}
	public void setM(int M) {
		this.M = M;
	}

	public void setSP(int sp) {
		this.sp = sp;
	}

	public void setLambdaAverage(double lambdaAverage) {
		this.lambdaAverage = lambdaAverage;
	}

	public void setLambdaUpper(double lambdaUpper) {
		this.lambdaUpper = lambdaUpper;
	}

	public void setLambdaLower(double lambdaLower) {
		this.lambdaLower = lambdaLower;
	}

	public void setLHName(String LHName) {
		this.LHName = LHName;
	}

	public void setDirectory(ArrayList<Integer> Directory) {
		this.Directory = new ArrayList<Integer>(Directory);
	}
	public void clearDirectory(){
		this.Directory.clear();
	}

	public void print() {
		System.out.println("Directory of Chains: " + this.Directory);
		System.out.println("M: " + this.M);
		System.out.println("sp: " + this.sp);
		System.out.println("totalPages is : " + this.totalPages);
		System.out.println("lambda Average: " + this.lambdaAverage);
		System.out.println("lambda Upper: " + this.lambdaUpper);
		System.out.println("lambda Lower: " + this.lambdaLower);
		System.out.println("LH name: " + this.LHName);

	}

	public LinearHashingXMLParser(String lhashingConfigFileName) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new File(lhashingConfigFileName));

			this.M = Integer.parseInt(document.getElementsByTagName("M").item(0).getTextContent());
			this.sp = Integer.parseInt(document.getElementsByTagName("sp").item(0).getTextContent());
			this.totalPages = Integer.parseInt(document.getElementsByTagName("totalPages").item(0).getTextContent());
			this.pageCapacity = Integer.parseInt(document.getElementsByTagName("pageCapacity").item(0).getTextContent());
			this.LHName = document.getElementsByTagName("LHName").item(0).getTextContent();
			this.lambdaAverage = Double
					.parseDouble(document.getElementsByTagName("lambdaAverage").item(0).getTextContent());
			this.lambdaUpper = Double
					.parseDouble(document.getElementsByTagName("lambdaUpper").item(0).getTextContent());
			this.lambdaLower = Double
					.parseDouble(document.getElementsByTagName("lambdaLower").item(0).getTextContent());
			System.out.println("lambdaLower:" + this.lambdaLower);

			String s = document.getElementsByTagName("Directory").item(0).getTextContent();
			if(s.length()>2){
				String[] ss = s.substring(1, s.length() - 1).split(", ");
				for (int i = 0; i < ss.length; i++) {
					this.Directory.add(Integer.parseInt(ss[i]));
				//	System.out.println("Directory from lhash file:" + Arrays.toString(this.Directory.toArray()));
				}
			}else{
				System.out.println("the directory in the lhashing file is null:");
			}
			
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void storeToXml(String lhashingConfigFileName) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new File(lhashingConfigFileName));
			document.getElementsByTagName("M").item(0).setTextContent(String.valueOf(this.M));
			document.getElementsByTagName("sp").item(0).setTextContent(String.valueOf(this.sp));
			document.getElementsByTagName("totalPages").item(0).setTextContent(String.valueOf(this.totalPages));

			document.getElementsByTagName("pageCapacity").item(0).setTextContent(String.valueOf(this.pageCapacity));
			document.getElementsByTagName("lambdaAverage").item(0).setTextContent(String.valueOf(this.lambdaAverage));
			document.getElementsByTagName("lambdaUpper").item(0).setTextContent(String.valueOf(this.lambdaUpper));
			document.getElementsByTagName("lambdaLower").item(0).setTextContent(String.valueOf(this.lambdaLower));
			document.getElementsByTagName("LHName").item(0).setTextContent(String.valueOf(this.LHName));
			document.getElementsByTagName("Directory").item(0).setTextContent(Arrays.toString(this.Directory.toArray()));

			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(document);
			StreamResult result = new StreamResult(new File(lhashingConfigFileName));
			transformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
