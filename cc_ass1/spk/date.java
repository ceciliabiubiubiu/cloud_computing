public class date {
	private static SimpleDateFormat dateformat = new SimpleDateFormat("dd.mm.yy");
	private String dateStr;
	private Date date;


	public date(String d){
		dateStr=d;
	try{
		date = dateformat.parse(dateStr)
	}catch(ParseException e){
		date=null;
	}
	}

	public String toString() {
		return dateformat.format(date);
	}


	public Date str_to_date(String d){
		return date;
	}
}