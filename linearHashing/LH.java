package SZ_LH.linearHashing;
/**
 * @author Shu Zhang
 */
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import storagemanager.StorageDirectoryDocument;
import storagemanager.StorageManagerClient;
import storagemanager.StorageUtils;

/**
 * The LH class implements linear hashing
 * 
 */
public class LH {
	//instance varaibles for storage utils
	private StorageUtils sbStoUtils;
	private StorageManagerClient sbStoMgrClient;
	private StorageDirectoryDocument xmlParser;
	private Tuple tupleDefinition;
    //instance variables for insertion and split
	private String name;
	private int tupleLength = Constant.TUPLE_LEN;
	private int rootPageId = -1;
	private int pageCapacity = 256;
	private ArrayList<Integer> Directory;
	private int M;
	private int sp = 0;
	private double lambdaAverage = 1.0;
	private double lambdaUpper;
	private double lambdaLower;
	private int totalPages = 0;
    // instance variables for producer
	private int producerPageId = -1;
	private int producerNextPageId = -1;
	private int producerTupleLen = Constant.TUPLE_LEN;
	private int producerTupleNum = 0;
	private int producerAlreadyRead = 0;
    //instance varaibles for consumer and transformer
	private int new0PageNum = 0;
	private int new1PageNum = 0;
	private int pageNum = 0;
	private int tupleNumOfnew0 = 0;
	private int tupleNumOfnew1 = 0;
	private int new0PageId;
	private int new1PageId;
	private int nextNew0PageId;
	private int nextNew1PageId;
	private int new0PageLoad = Constant.PAGE_HEADER_SIZE;
	private int new1PageLoad = Constant.PAGE_HEADER_SIZE;
	private int rootNew0PageId, rootNew1PageId;

	/**
	 * Construct a linear hashing object
	 */
	public LH(StorageUtils stoUtils, Tuple tupleDefinition) {
		//build all storage related utils
		this.sbStoUtils = stoUtils;
		this.sbStoMgrClient = this.sbStoUtils.getXmlClient();
		this.xmlParser = this.sbStoUtils.getXmlParser();
		this.tupleDefinition = tupleDefinition;
		this.Directory = new ArrayList<Integer>();

	}

	/**
	 * initialize LH when lhashingConfigFile is not written before
	 */
	public void initializeLHWithoutPageIdsInLHConfig(String lhashingConfigFile, int initialM, double lambdaMax,
			double lambdaMin) {
		//user speficied parameters for creating a LH object
		this.M = initialM;
		this.lambdaUpper = lambdaMax;
		this.lambdaLower = lambdaMin;
		this.totalPages = totalPages;

		// create the original M root chains, add to Directory
		for (int i = 0; i < M; i++) {
			this.Directory.add(createNewPage());
			this.totalPages++;

		}
        // some basic information is parsed from config file
		LinearHashingXMLParser lhashingXmlParser = new LinearHashingXMLParser(lhashingConfigFile);
		this.pageCapacity = lhashingXmlParser.getPageCapacity();
		this.name = lhashingXmlParser.getName();
		this.sp = lhashingXmlParser.getSP();
		this.lambdaAverage = lhashingXmlParser.getLambdaAverage();
		System.out.println("Create LH from lhashing file empty : ");
	}

	/**
	 * initialize LH when lhashingConfigFile has already been written before
	 */
	public void initializeLHWithPageIdsInLHConfig(String lhashingConfigFile) {
        // create a parser, and parse all parameters from file
		LinearHashingXMLParser lhashingXmlParser = new LinearHashingXMLParser(lhashingConfigFile);

		this.Directory = lhashingXmlParser.getDirectory();
		this.pageCapacity = lhashingXmlParser.getPageCapacity();
		this.name = lhashingXmlParser.getName();
		this.sp = lhashingXmlParser.getSP();
		this.totalPages = lhashingXmlParser.getTotalPages();
		this.lambdaAverage = lhashingXmlParser.getLambdaAverage();
		this.M = lhashingXmlParser.getM();
		this.lambdaUpper = lhashingXmlParser.getLambdaUpper();
		this.lambdaLower = lhashingXmlParser.getLambdaLower();
		System.out.println("Initialize LH from exsting lhashing file: ");
		// System.out.println("Directory is : " +
		// Arrays.toString(this.Directory.toArray()));
		// System.out.println("M is : " + this.M);
		// System.out.println("lambdaAverage is: " + this.lambdaAverage);
	}

	/**
	 * Insert one byte array record into current LH object
	 */
	public void LHashingInsert(byte[] record) throws IOException {
		// get the key of a record
		byte[] key = this.tupleDefinition.generateKey(record);
		int keyInt = ReWriUtils.byteArrayToInt(key, 0);

		// find the chain
		int chainPosition = keyInt % this.M;
		if (chainPosition < sp) {
			chainPosition = keyInt % (2 * this.M);
		}
		System.out.println("chainPosition is : " + chainPosition);

		// Compare the record in the chain with the new record to find the position to insert
		int rootPageId = this.Directory.get(chainPosition);
		System.out.println("rootPageId for the selected chain is : " + rootPageId);

		int curPageID = rootPageId;
		byte[] buffer = this.sbStoMgrClient.readPagewithPin(curPageID);
		int nextPageID = -1;
		int tupleNum;
		int offset = Constant.PAGE_HEADER_SIZE;

		int pageLoad = Constant.PAGE_HEADER_SIZE;
		boolean offsetFound = false;
		byte[] tempRecord = new byte[this.tupleLength];
		byte[] insertRecord = new byte[this.tupleLength];

		for (int ii = 0; ii < this.tupleLength; ii++) {
			insertRecord[ii] = record[ii];
		}
		System.out.println("The record to insert is : " + Arrays.toString(insertRecord));

		//iterate each page in the chain
		do {
			int tupleRead = 0;
			offset = Constant.PAGE_HEADER_SIZE;
			tupleNum = ReWriUtils.byteArrayToInt(buffer, Constant.TUPLE_NUM_PTR);
			System.out.println("Check tupleNum from current page is : " + tupleNum);

			//iterate each tuple in current page
			for (int i = 0; i < tupleNum; i++) {
				
				// compare the read tuple with record
				if (!offsetFound) {
					//check if the position is found or not
					offsetFound = this.tupleDefinition.compare(buffer, record, offset) > 0;
					System.out.println("offsetFound for current page ? : " + offsetFound);
					
				} else {
					//move the tuple one tuple forward and insert the record
					for (int ii = 0; ii < this.tupleLength; ii++) {
						tempRecord[ii] = buffer[offset + ii];
					}
					for (int jj = 0; jj < this.tupleLength; jj++) {
						buffer[offset + jj] = insertRecord[jj];
					}
					for (int kk = 0; kk < this.tupleLength; kk++) {
						insertRecord[kk] = tempRecord[kk];
					}
				}

				tupleRead++;
				offset = Constant.PAGE_HEADER_SIZE + tupleRead * this.tupleLength;
				System.out.println("The offset after reading tupleRead: " + tupleRead + " is " + offset);
			}
			
			if (offsetFound) {
				// write the current page
				try {
					this.sbStoMgrClient.writePagewithoutPin(curPageID, buffer);
					this.sbStoMgrClient.flushBuffer();
				} catch (IOException e) {
					System.out.println("write page error");
				}
			}

			nextPageID = ReWriUtils.byteArrayToInt(buffer, Constant.NEXT_PAGE_PTR);
			System.out.println("The nextPageID from current page is : " + nextPageID);

			if (nextPageID != -1) {
				curPageID = nextPageID;
				buffer = this.sbStoMgrClient.readPagewithPin(curPageID);
				System.out.println("reading new buffer page in insert");

			}

		} while (nextPageID != -1);

		if (tupleNum < Constant.TUPLE_NUM) {
			// write the last tuple in the current page
			System.out.println("Write the last tuple to the last page to disk.");
			for (int jj = 0; jj < this.tupleLength; jj++) {
				buffer[offset + jj] = insertRecord[jj];
			}
			
			//update the page info 
			tupleNum++;
			offset += this.tupleLength;
			ReWriUtils.intToByteArray(tupleNum, buffer, Constant.TUPLE_NUM_PTR);
			ReWriUtils.intToByteArray(offset, buffer, Constant.NUMOFBYTESUSED_PTR);

			try {
				this.sbStoMgrClient.writePagewithoutPin(curPageID, buffer);
				this.sbStoMgrClient.flushBuffer();
			} catch (IOException e) {
				System.out.println("write page error");
			}

		} else {
			// write the last tuple to a new allocated page
			int nextNewPageId = createNewPage();
			this.totalPages++;
			// write current page to disk
			ReWriUtils.intToByteArray(nextNewPageId, buffer, Constant.NEXT_PAGE_PTR);
			try {
				this.sbStoMgrClient.writePagewithoutPin(curPageID, buffer);
				this.sbStoMgrClient.flushBuffer();
			} catch (IOException e) {
				System.out.println("write page error");
			}

			byte[] bufferNew;

			try {
				bufferNew = this.sbStoMgrClient.readPagewithoutPin(nextNewPageId);
			} catch (IOException e) {
				bufferNew = new byte[0];
				System.out.println("read page error");
			}

			offset = Constant.PAGE_HEADER_SIZE;
			tupleNum = 0;

			// add one tuple to new added page
			System.arraycopy(record, 0, bufferNew, offset, this.tupleLength);
			tupleNum++;
			offset += this.tupleLength;
			ReWriUtils.intToByteArray(tupleNum, bufferNew, Constant.TUPLE_NUM_PTR);
			ReWriUtils.intToByteArray(-1, bufferNew, Constant.NEXT_PAGE_PTR);
			ReWriUtils.intToByteArray(offset, bufferNew, Constant.NUMOFBYTESUSED_PTR);

			try {
				this.sbStoMgrClient.writePagewithoutPin(nextNewPageId, bufferNew);
				this.sbStoMgrClient.flushBuffer();
			} catch (IOException e) {
				System.out.println("write page error");
			}

		}

		// update parameters and check for split condition
		this.lambdaAverage = this.totalPages * 1.0 / this.Directory.size();
		System.out.println("The lambdaAverage after inserting one tuple is : " + this.lambdaAverage);

		while (this.lambdaAverage > this.lambdaUpper) {
			
			//split chain
			split();

			// update parameters
			this.Directory.add(this.rootNew1PageId);
			this.Directory.set(this.sp, this.rootNew0PageId);
			System.out.println("The Directory updated after split is : " + Arrays.toString(this.Directory.toArray()));

			if (this.sp < this.M - 1) {
				this.sp++;
			} else {
				this.M = 2 * this.M;
				this.sp = 0;
			}

			System.out.println("The lambdaAverage after splitChain once done is : " + this.lambdaAverage);

		}
		System.out.println("Insertion of one tuple is done!");
	}

	/**
	 * Create a new page and write some basic info into page
	 */
	public int createNewPage() {
		
		int newLeafId = this.sbStoMgrClient.allocatePage();

		byte[] buffer;

		try {
			buffer = this.sbStoMgrClient.readPagewithoutPin(newLeafId);
		} catch (IOException e) {
			buffer = new byte[0];
			System.out.println("read page error");
			return -1;
		}

		ReWriUtils.intToByteArray(Constant.PAGE_HEADER_SIZE, buffer, Constant.NUMOFBYTESUSED_PTR);
		ReWriUtils.intToByteArray(-1, buffer, Constant.NEXT_PAGE_PTR);
		ReWriUtils.intToByteArray(0, buffer, Constant.TUPLE_NUM_PTR);

		try {
			this.sbStoMgrClient.writePagewithoutPin(newLeafId, buffer);
			this.sbStoMgrClient.flushBuffer();
		} catch (IOException e) {
			System.out.println("write page error");
		}
		// this.totalPages++;
		return newLeafId;

	}

	/**
	 * update linear hashing configure file
	 */
	public void updateLHConfigAndStorageCatalog(String lhashingConfigFile) {

		//store the current values of all the relevant parameters into file
		LinearHashingXMLParser lhXmlParser = new LinearHashingXMLParser(lhashingConfigFile);
		lhXmlParser.setDirectory(this.Directory);
		lhXmlParser.setM(this.M);
		lhXmlParser.setSP(this.sp);
		lhXmlParser.setTotalPages(this.totalPages);
		lhXmlParser.setLambdaUpper(this.lambdaUpper);
		lhXmlParser.setLambdaAverage(this.lambdaAverage);
		lhXmlParser.setLambdaLower(this.lambdaLower);
		
		lhXmlParser.storeToXml(lhashingConfigFile);
		xmlParser.removeXMLNode(this.name);
		xmlParser.addXMLDocument(this.name, this.name, String.valueOf(rootPageId), String.valueOf(1));

		try {
			xmlParser.writeXmlFile(new File(this.sbStoUtils.xmlCatalog_File_Path), xmlParser.getXMLDocument());
		} catch (Exception e) {
			System.out.println("Failed on updating catalog file!");
		}
	}

	/**
	 * clear the updated info and set back to initial values
	 */
	public void clearLHConfigAndStorageCatalog(String lhashingConfigFile) {

		LinearHashingXMLParser lhXmlParser = new LinearHashingXMLParser(lhashingConfigFile);

		lhXmlParser.setSP(0);
		lhXmlParser.setM(-1);
		lhXmlParser.setTotalPages(0);
		lhXmlParser.setLambdaUpper(1.5);
		lhXmlParser.setLambdaLower(1.25);
		lhXmlParser.setLambdaAverage(1.0);
		lhXmlParser.clearDirectory();
		lhXmlParser.storeToXml(lhashingConfigFile);
		xmlParser.removeXMLNode(this.name);

		try {
			xmlParser.writeXmlFile(new File(this.sbStoUtils.xmlCatalog_File_Path), xmlParser.getXMLDocument());
		} catch (Exception e) {
			System.out.println("Failed on updating catalog file!");
		}
	}

	/**
	 * prepare for producer iterator
	 */
	public boolean producerOpen(int PageID) {
		this.producerPageId = PageID;
		if (this.producerPageId <= 0) {
			return false;
		}
		byte[] buffer;
		try {
			buffer = this.sbStoMgrClient.readPagewithoutPin(this.producerPageId);
		} catch (IOException e) {
			buffer = new byte[0];
			System.out.println("read page error");
		}
		this.producerNextPageId = ReWriUtils.byteArrayToInt(buffer, Constant.NEXT_PAGE_PTR);
     	this.producerTupleNum = ReWriUtils.byteArrayToInt(buffer, Constant.TUPLE_NUM_PTR);
		this.producerAlreadyRead = 0;
		this.pageNum = 1;
		return true;
	}

	/**
	 * check if the producer has next tuple or not
	 */
	public boolean producerHasNext() {
		if ((this.producerAlreadyRead >= this.producerTupleNum) && (this.producerNextPageId == -1)) {
			return false;
		}
		return true;
	}

	/**
	 * produce the next tuple
	 */
	public byte[] producerGetNext() {
		if (!producerHasNext()) {
			return null;
		}
		
		//update producer page id if finished reading the current page 
		byte[] tuple = new byte[this.producerTupleLen];
		if (this.producerAlreadyRead >= this.producerTupleNum) {
			System.out.println("producerPageId before unpin this page is: " + this.producerPageId);

			try {
				this.sbStoMgrClient.unpinPage(this.producerPageId);

			} catch (IOException e) {
				System.out.println("flush buffer error");
			}

			this.producerPageId = this.producerNextPageId;
			this.producerAlreadyRead = 0;
			this.pageNum++;
			System.out.println("producerNextPageId is: " + this.producerNextPageId);

		}

		byte[] buffer;
		try {
			buffer = this.sbStoMgrClient.readPagewithoutPin(this.producerPageId);
		} catch (IOException e) {
			buffer = new byte[0];
			System.out.println("read page error");
		}

		this.producerNextPageId = ReWriUtils.byteArrayToInt(buffer, Constant.NEXT_PAGE_PTR);
		this.producerTupleNum = ReWriUtils.byteArrayToInt(buffer, Constant.TUPLE_NUM_PTR);
		System.out.println("producerTupleNum is: " + this.producerTupleNum);

		int offset = Constant.PAGE_HEADER_SIZE + this.producerAlreadyRead * this.producerTupleLen;
		System.out.println("offset in current producer page is: " + offset);

		//produce the next tuple
		for (int kk = 0; kk < this.producerTupleLen; kk++) {
			tuple[kk] = buffer[offset + kk];
		}
		this.producerAlreadyRead += 1;
		System.out.println("producerAlreadyRead is: " + this.producerAlreadyRead);

		return tuple;

	}

	/**
	 * set producer variables to initial values
	 */
	public void producerClose() {
		this.producerPageId = -1;
		this.producerNextPageId = -1;
		this.producerTupleNum = 0;
		this.producerAlreadyRead = 0;
		// this.pageNum=0;
	}

	/**
	 * return the chain position the record will rehash into
	 */
	public int transformer(byte[] record) {
		System.out.println("The tuple to be rehashed is : " + Arrays.toString(record));

		byte[] key = this.tupleDefinition.generateKey(record);
		int keyInt = ReWriUtils.byteArrayToInt(key, 0);

		// find the chain
		int chainIndex = keyInt % (2 * this.M);
		System.out.println("The chainPosition of the rehashed tuple split is : " + chainIndex);

		return chainIndex;

	}

	/**
	 * prepare for consumer iterator, get two new chains ready
	 */
	public boolean consumerOpen() {

		// create two new chain root id, one for replaceing the splitting chain
		this.rootNew0PageId = createNewPage();
		this.new0PageNum++;
		this.rootNew1PageId = createNewPage();
		this.new1PageNum++;
		System.out.println("rootNew0PageId is: " + this.rootNew0PageId + "rootNew1PageId is : " + this.rootNew1PageId);
		System.out.println("sp is: " + this.sp);

		this.new0PageId = this.rootNew0PageId;
		this.new1PageId = this.rootNew1PageId;

		this.tupleNumOfnew0 = 0;
		this.tupleNumOfnew1 = 0;

		this.new0PageLoad = Constant.PAGE_HEADER_SIZE;
		this.new1PageLoad = Constant.PAGE_HEADER_SIZE;

		this.nextNew0PageId = -1;
		this.nextNew1PageId = -1;

		return true;

	}

	/**
	 * consumer iterator, write the given tuple into its destination
	 */
	public void consumerNext(byte[] tuple, int chainPos) {
		byte[] bufferNew0;
		byte[] bufferNew1;

		try {
			bufferNew0 = this.sbStoMgrClient.readPagewithoutPin(this.rootNew0PageId);
		} catch (IOException e) {
			bufferNew0 = new byte[0];
			System.out.println("read page error");
		}
		try {
			bufferNew1 = this.sbStoMgrClient.readPagewithoutPin(this.rootNew1PageId);
		} catch (IOException e) {
			bufferNew1 = new byte[0];
			System.out.println("read page error");
		}
		// rehash the record into one of the chains specified by the chainPosition
		int chainPosition = chainPos;
		byte[] record = new byte[this.tupleLength];
		for (int ii = 0; ii < this.tupleLength; ii++) {
			record[ii] = tuple[ii];
		}

		// write it to bufferNew0
		if (chainPosition == this.sp) {
			System.out.println("write it to bufferNew0!");
			if (this.tupleNumOfnew0 < Constant.TUPLE_NUM) {
				System.arraycopy(record, 0, bufferNew0, this.new0PageLoad, this.tupleLength);
				this.tupleNumOfnew0++;
				ReWriUtils.intToByteArray(this.tupleNumOfnew0, bufferNew0, Constant.TUPLE_NUM_PTR);
				this.new0PageLoad = this.new0PageLoad + this.tupleLength;

			} else {
				this.nextNew0PageId = createNewPage();

				// update the header of the current page
				ReWriUtils.intToByteArray(this.tupleNumOfnew0, bufferNew0, Constant.TUPLE_NUM_PTR);
				ReWriUtils.intToByteArray(this.new0PageLoad, bufferNew0, Constant.NUMOFBYTESUSED_PTR);
				ReWriUtils.intToByteArray(this.nextNew0PageId, bufferNew0, Constant.NEXT_PAGE_PTR);
				this.new0PageNum++;

				// write the current page
				System.out.println("finished one page in new chain 0, write it to disk!");
				try {
					this.sbStoMgrClient.writePagewithoutPin(this.new0PageId, bufferNew0);
				} catch (IOException e) {
					System.out.println("write page error");
				}

				// initialize next page
				this.tupleNumOfnew0 = 0;
				this.new0PageLoad = Constant.PAGE_HEADER_SIZE;
				if (this.nextNew0PageId != -1) {
					this.new0PageId = this.nextNew0PageId;
				}
				// write data to the next page
				System.arraycopy(record, 0, bufferNew0, this.new0PageLoad, this.tupleLength);
				this.new0PageLoad = this.new0PageLoad + this.tupleLength;
				this.tupleNumOfnew0++;

			}

		}

		// write it to bufferNew1
		if (chainPosition == (this.sp + this.M)) {
			System.out.println("write it to bufferNew1!");

			if (this.tupleNumOfnew1 < Constant.TUPLE_NUM) {
				System.arraycopy(record, 0, bufferNew1, this.new1PageLoad, this.tupleLength);
				this.tupleNumOfnew1++;
				ReWriUtils.intToByteArray(this.tupleNumOfnew1, bufferNew1, Constant.TUPLE_NUM_PTR);
				this.new1PageLoad = this.new1PageLoad + this.tupleLength;

			} else {
				this.nextNew1PageId = createNewPage();

				// write the header of the current page
				ReWriUtils.intToByteArray(this.tupleNumOfnew1, bufferNew1, Constant.TUPLE_NUM_PTR);
				ReWriUtils.intToByteArray(this.new1PageLoad, bufferNew1, Constant.NUMOFBYTESUSED_PTR);
				ReWriUtils.intToByteArray(this.nextNew1PageId, bufferNew1, Constant.NEXT_PAGE_PTR);
				this.new1PageNum++;

				// write the current page
				System.out.println("finished one page in new chain 1, write it to disk!");
				try {
					this.sbStoMgrClient.writePagewithoutPin(this.new1PageId, bufferNew1);
				} catch (IOException e) {
					System.out.println("write page error");
				}

				// initialize next page
				this.tupleNumOfnew1 = 0;
				this.new1PageLoad = Constant.PAGE_HEADER_SIZE;
				if (this.nextNew1PageId != -1) {
					this.new1PageId = this.nextNew1PageId;
				}
				// write data to the next page
				System.arraycopy(record, 0, bufferNew1, this.new1PageLoad, this.tupleLength);
				this.new1PageLoad = this.new1PageLoad + this.tupleLength;
				this.tupleNumOfnew1++;

			}

		}
	}

	/**
	 * finish up wrting the last pages for the two new chains
	 */
	public void consumerClose() {
		byte[] bufferNew0;
		byte[] bufferNew1;

		try {
			bufferNew0 = this.sbStoMgrClient.readPagewithoutPin(this.new0PageId);
		} catch (IOException e) {
			bufferNew0 = new byte[0];
			System.out.println("read page error");
		}
		try {
			bufferNew1 = this.sbStoMgrClient.readPagewithoutPin(this.new1PageId);
		} catch (IOException e) {
			bufferNew1 = new byte[0];
			System.out.println("read page error");
		}

		if (this.nextNew0PageId == -1) {
			System.err.println("nextNew0PageId List has only one page.");
		} else if (this.nextNew1PageId == -1) {
			System.err.println("nextNew1PageId List has only one page.");
		} else {
			this.nextNew0PageId = -1;
			this.nextNew1PageId = -1;
		}

		// write the last page of new0 chain List

		System.out.println("Write the last page of new0 List.");
		ReWriUtils.intToByteArray(this.nextNew0PageId, bufferNew0, Constant.NEXT_PAGE_PTR);
		ReWriUtils.intToByteArray(this.new0PageLoad, bufferNew0, Constant.NUMOFBYTESUSED_PTR);
		ReWriUtils.intToByteArray(this.tupleNumOfnew0, bufferNew0, Constant.TUPLE_NUM_PTR);
		try {
			this.sbStoMgrClient.writePagewithoutPin(this.new0PageId, bufferNew0);
		} catch (IOException e) {
			System.out.println("write page error");
		}

		// write the last page of new1 chain List
		System.out.println("Write the last page of new1 List.");
		ReWriUtils.intToByteArray(this.nextNew1PageId, bufferNew1, Constant.NEXT_PAGE_PTR);
		ReWriUtils.intToByteArray(this.new1PageLoad, bufferNew1, Constant.NUMOFBYTESUSED_PTR);
		ReWriUtils.intToByteArray(this.tupleNumOfnew1, bufferNew1, Constant.TUPLE_NUM_PTR);
		try {
			this.sbStoMgrClient.writePagewithoutPin(this.new1PageId, bufferNew1);
		} catch (IOException e) {
			System.out.println("write page error");
		}

		// flush the buffer(s) of the buffer manager to the disk
		try {
			this.sbStoMgrClient.flushBuffer();
		} catch (IOException e) {
			System.out.println("flushBuffer  error");
		}
		// update the bitmap
		this.sbStoMgrClient.writeBitMap();

		this.new0PageNum = 0;
		this.new1PageNum = 0;

	}

	/**
	 * split the sp chain
	 */
	public void split() {
		System.out.println("split starts!");

		int rootpageId = this.Directory.get(this.sp);
		System.out.println("rootPageId is : " + rootpageId);

		if (producerOpen(rootpageId) && consumerOpen()) {
			while (producerHasNext()) {
				byte[] record = producerGetNext();

				int chainPosition = transformer(record);
				System.out.println("chainPosition is : " + chainPosition);

				System.out.println("one consumerNext starts!");
				consumerNext(record, chainPosition);
				System.out.println("one consumerNext ends!");
			}
			System.out.println("producer has no next anymore!");

			// update parameters

			System.out.println("deallocated pageNum: " + this.pageNum);
			System.out.println("allocated new0PageNum is : " + this.new0PageNum);
			System.out.println("allocated new1PageNum is : " + this.new1PageNum);

			this.totalPages = this.totalPages + this.new0PageNum + this.new1PageNum - this.pageNum;
			this.lambdaAverage = this.totalPages * 1.0 / this.Directory.size();
			System.out.println("The lambdaAverage after splitChain is done: " + this.lambdaAverage);
			System.out.println("The totalPages after splitChain is done : " + this.totalPages);

			consumerClose();
			System.out.println("consumerClose!");
			producerClose();
			System.out.println("producerClose!");

		}

	}

}
