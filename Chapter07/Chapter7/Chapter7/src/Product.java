import java.util.ArrayList;

public class Product {
	int Id;
	String ASIN;
	String title;
	String group;
	int salesrank;
	int simCount;
	
	String[] similar;
	int catCount;
	ArrayList<Category> categories;
	ReviewMetaData rmd;
	ArrayList<Review> reviews = new ArrayList<Review>();
	public int getSimCount() {
		return simCount;
	}
	public void setSimCount(int simCount) {
		this.simCount = simCount;
	}
	public ArrayList<Review> getReviews() {
		return reviews;
	}
	public void setReviews(ArrayList<Review> reviews) {
		this.reviews = reviews;
	}
	public void addReviews(Review review){
		reviews.add(review);
	}
	public ReviewMetaData getRmd() {
		return rmd;
	}
	public void setRmd(ReviewMetaData rmd) {
		this.rmd = rmd;
	}
	ArrayList<Review> review;
	public int getId() {
		return Id;
	}
	public void setId(int id) {
		Id = id;
	}
	public String getASIN() {
		return ASIN;
	}
	public void setASIN(String aSIN) {
		ASIN = aSIN;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public int getSalesrank() {
		return salesrank;
	}
	public void setSalesrank(int salesrank) {
		this.salesrank = salesrank;
	}
	public String[] getSimilar() {
		return similar;
	}
	public void setSimilar(String[] similar) {
		this.similar = similar;
	}
	public int getCatCount() {
		return catCount;
	}
	public void setCatCount(int catCount) {
		this.catCount = catCount;
		categories = new ArrayList<Category>();
	}
	public ArrayList<Category> getCategories() {
		return categories;
	}
	public void setCategories(ArrayList<Category> categories) {
		this.categories = categories;
	}
	public void addCategories(Category category){
		categories.add(category);
	}
	public ArrayList<Review> getReview() {
		return review;
	}
	public void setReview(ArrayList<Review> review) {
		this.review = review;
	}
	public String convertToJson(boolean discontinued){
		StringBuffer jsonStringBuffer=new StringBuffer();
		jsonStringBuffer.append("{\"Id\":").append(this.getId()).append(",").append("\"ASIN\":").append("\"").append(this.getASIN()).append("\"");
		if (!discontinued){
			jsonStringBuffer.append(",");
			String t = this.getTitle().replaceAll("\"", " ").replaceAll("\\\\", " ").replaceAll("\t", " ");
			
			jsonStringBuffer.append("\"title\":").append("\"").append(t).append("\"").append(",");
			jsonStringBuffer.append("\"group\":").append("\"").append(this.getGroup()).append("\"").append(",").append("\"salerank\":").append(this.getSalesrank()).append(",");
			String[] similars=this.getSimilar();
			jsonStringBuffer.append("\"similars\":").append(this.getSimCount()).append(",");
			if (this.getSimCount()>0){
				jsonStringBuffer.append("\"similarLines\":").append("[");
				for (int i=0; i<similars.length;i++){
					jsonStringBuffer.append("{\"similar\":").append("\"").append(similar[i]).append("\"").append("}");
					if (i<similars.length-1){
						jsonStringBuffer.append(",");
					}
				}
				jsonStringBuffer.append("],");
			}
			jsonStringBuffer.append("\"categories\":").append(this.getCatCount()).append(",");
			if (this.getCatCount() > 0) {
				ArrayList<Category> categories = this.getCategories();
				jsonStringBuffer.append("\"categoryLines\": ");
				jsonStringBuffer.append("[");
				for (int i=0; i<categories.size();i++){
					Category c = categories.get(i);
					String[] fields = c.getFields();
					for (int j=0; j<fields.length;j++){
						String s = fields[j].replace("\\", "/");
						jsonStringBuffer.append("{\"category\":").append("\"").append(s).append("\"").append("}");
						if (j<fields.length-1){
							jsonStringBuffer.append(",");
						}
					}
					jsonStringBuffer.append("],");
				}
			}
			jsonStringBuffer.append("\"ReviewMetaData\": {");
			ReviewMetaData rmd = this.getRmd();
			jsonStringBuffer.append("\"total\":").append(rmd.getTotal()).append(",");
			jsonStringBuffer.append("\"downloaded\":").append(rmd.getDownloaded()).append(",");
			jsonStringBuffer.append("\"avg_rating\":").append(rmd.getAvgRating()).append("}");
			if (rmd.getDownloaded()!=0){
				jsonStringBuffer.append(",");
				ArrayList<Review> reviews = this.getReviews();
				jsonStringBuffer.append("\"reviewLines\":").append("[");
				for (int j=0; j<reviews.size();j++){
					Review r = reviews.get(j);
					jsonStringBuffer.append("{\"review\":{").append("\"date\":").append("\"").append(r.getDate()).append("\",");
					jsonStringBuffer.append("\"customerId\":").append("\"").append(r.getCustomerId()).append("\",");
					jsonStringBuffer.append("\"rating\":").append(r.getRating()).append(",");
					jsonStringBuffer.append("\"votes\":").append(r.getVotes()).append(",");
					jsonStringBuffer.append("\"helpful\":").append(r.getHelpful()).append("}");
					jsonStringBuffer.append("}");
					if (j<reviews.size()-1){
						jsonStringBuffer.append(",");
					}
				}
				
				jsonStringBuffer.append("]");
			}
		}
		jsonStringBuffer.append("}");
		return jsonStringBuffer.toString();
	}
}