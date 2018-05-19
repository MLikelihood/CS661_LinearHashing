package SZ_LH.linearHashing;

public class Constant {
	
	public static int PAGE_HEADER_SIZE = 12;// 4 for storing TUPLE_NUM, 4 for storing NEXT_PAGE id, 4 for storing NUMOFBYTESUSED
	
	public static int TUPLE_NUM=6; // (256-12)/35
	public static int TUPLE_LEN= 35; // see tuple configure file

	public static int TUPLE_NUM_PTR = 0; // the offset for accessing TUPLE_NUM
	public static int NEXT_PAGE_PTR = 4; // the offset for accessing NEXT_PAGE id
	public static int NUMOFBYTESUSED_PTR=8; // the offset for accessing NUMOFBYTESUSED
	
	//the first tuple will begin after the 12th bytes, keep growing
	public static int PAGE_SIZE_USED_PTR = 12;

	public static int ATTR_INTEGER = 0;
	
	public static int ATTR_STRING = 1;
	
	
}
