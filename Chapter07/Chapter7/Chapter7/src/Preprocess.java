import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

public class Preprocess {
	static int readCount=0;
	public static void main(String[] args) throws IOException{
		if (args.length < 2) {
			System.out.println("Correct usgae: java Preprocess your-input-file-with-full-path your-output-file-with-full-path");
			System.out.println("For example: java Preprocess /Users/aurobindosarkar/Downloads/amazon-meta.txt /Users/aurobindosarkar/Downloads/delaftertestinput.json");
			System.exit(0);
		}
		String cmdLineInFile = args[0];
		String cmdLineOutFile = args[1];
		boolean discontinued = false;
		BufferedReader br = new BufferedReader(new FileReader(cmdLineInFile));
		PrintWriter inputJson = new PrintWriter(cmdLineOutFile);
		System.out.println("Processing...");
		int writeCount=0;
		String line = "";
		Category[] categories;
		int categoryCount = 0;		
		int dropCount=0;
		Product p = null;
		while ((line=br.readLine())!=null){
			if (line.contains("discontinued product")){
				discontinued=true;
				String record=p.convertToJson(discontinued);
				inputJson.println(record);
				discontinued=false;
				p=null;
			} else {
				if ((line.contains("Id:")) && (!line.contains("title"))){
					if (p!=null){
						String record=p.convertToJson(discontinued);
							inputJson.println(record);
							writeCount++;
					}
					p = new Product();
					p.setId(Integer.parseInt(line.substring(line.indexOf("Id:")+3).trim()));
				} else if (line.contains("ASIN:")) {
					p.setASIN(line.substring(line.indexOf("ASIN:")+5).trim());
				} else if (line.contains("title:")) {
					p.setTitle(line.substring(line.indexOf("title:")+6).trim());
				} else if (line.contains("group:")) {
					p.setGroup(line.substring(line.indexOf("group:")+6).trim());
				} else if (line.contains("salesrank")){
					p.setSalesrank(Integer.parseInt(line.substring(line.indexOf("salesrank:")+10).trim()));
				} else if (line.contains("similar")){
					StringTokenizer st = new StringTokenizer(line.substring(line.indexOf("similar:")+8).trim());
					String[] similar = new String[st.countTokens()-1];
					int i = 0;
					int simCount = 0;
					int j = 0;
					while (st.hasMoreElements()) {
						if (i==0) {
							simCount = Integer.parseInt(st.nextElement().toString());
						} else {
							similar[j]=st.nextElement().toString();
							j++;
						}
						i++;
					}
					p.setSimCount(simCount);
					p.setSimilar(similar);
				} else if (line.contains("categories")){
					p.setCatCount(Integer.parseInt(line.substring(line.indexOf("categories:")+11).trim()));
					categories = new Category[p.getCatCount()];
					String[] lines = new String[p.getCatCount()];
					for (int i=0; i<p.getCatCount();i++){
						lines[i]=br.readLine();
						readCount++;
					}
					p=processCategories(p, p.getCatCount(), lines);
				} else if (line.contains("reviews:")) {
					ReviewMetaData rmd = new ReviewMetaData();
					String str = line.substring(line.indexOf("reviews:")+8).trim();
					int total = Integer.parseInt(str.substring(str.indexOf("total:")+6, str.indexOf("downloaded:")).trim());
					int downloaded = Integer.parseInt(str.substring(str.indexOf("downloaded:")+11, str.indexOf("avg rating:")).trim());
					float avgRating = Float.parseFloat(str.substring(str.indexOf("avg rating:")+11).trim());
					rmd.setTotal(total);
					rmd.setDownloaded(downloaded);
					rmd.setAvgRating(avgRating);
					p.setRmd(rmd);
					
					for (int i=0; i<downloaded;i++){
						line = br.readLine();
						readCount++;
						Review r = new Review();
						String date = line.substring(0, line.indexOf("cutomer:")).trim();
						String customerId = line.substring(line.indexOf("cutomer:")+8, line.indexOf("rating:")).trim();
						int rating = Integer.parseInt((line.substring(line.indexOf("rating:")+7, line.indexOf("votes:"))).trim());
						int votes = Integer.parseInt((line.substring(line.indexOf("votes:")+6, line.indexOf("helpful:"))).trim());
						int helpful = Integer.parseInt((line.substring(line.indexOf("helpful:")+8)).trim());
						r.setDate(date);
						r.setCustomerId(customerId);
						r.setRating(rating);
						r.setVotes(votes);
						r.setHelpful(helpful);
						p.addReviews(r);
					}
				}
			}
			readCount++;		
		}
		
		if (p!=null){
			String record=p.convertToJson(discontinued);
			inputJson.println(record);
		}
		System.out.println("readCount: " + readCount);
		System.out.println("dropCount: " + dropCount);
		br.close();
		inputJson.flush();
		inputJson.close();
	}
	
	public static Product processCategories(Product p, int catCount, String[] lines) {		
		int i = 0;
		String[] fields = new String[catCount];
		while (i < catCount){
			String line = lines[i].trim();
			int k = 0;
			fields[i]=line;
			i++;
		}
		Category c = new Category();
		c.setFields(fields);
		p.addCategories(c);
		return p;
	}
}

