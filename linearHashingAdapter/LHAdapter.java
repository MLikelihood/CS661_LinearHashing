package SZ_LH.linearHashingAdapter;

import java.io.IOException;
import java.util.Arrays;

import cysystem.clientsmanager.ClientsFactory;
import cysystem.clientsmanager.CyGUI;
import cysystem.diwGUI.gui.DBGui;
import storagemanager.StorageUtils;

import SZ_LH.linearHashing.LH;
import SZ_LH.linearHashing.LHTupleGenerator;
import SZ_LH.linearHashing.LinearHashingXMLParser;
import SZ_LH.linearHashing.ReWriUtils;
import SZ_LH.linearHashing.Tuple;
import SZ_LH.linearHashing.TupleAttribute;
import SZ_LH.linearHashing.LHPrepareSortedData;

public class LHAdapter extends ClientsFactory {
	private StorageUtils stoUtils;

	public void initialize(CyGUI gui, int clientID) {
		this.dbgui = gui;
		this.stoUtils = ((DBGui) dbgui).getStorageUtils();
	}

	public void execute(int clientID, String text) {
		if (this.dbgui == null) {
			System.out.println("Error! The client parser is not initialized properly."
					+ " The handle to CyDIW GUI is not initialized.");
			return;
		}

		text = text.trim();
		String[] commands = text.split(" ");

		// List Commands
		if (commands[0].equalsIgnoreCase("list") && commands[1].equalsIgnoreCase("commands")) {

			dbgui.addOutputPlainText("$SZ_LH Commands List:");
			dbgui.addOutputPlainText("$SZ_LH:> list commands;");
			dbgui.addOutputPlainText(
					"$SZ_LH:> LHCreateEmpty <LHConfigXmlFile> <TupleConfigXmlFile> <M> <lambdaUpper> <lambdaLower> <totalPages>;");
			dbgui.addOutputPlainText("$SZ_LH:> LHPrepareSortedData <TupleConfigXmlFile> <TupleTxtFile>;");
			dbgui.addOutputPlainText("$SZ_LH:> LHInsert <LHConfigXmlFile> <TupleConfigXmlFile> [OneTuple];");
			dbgui.addOutputPlainText("$SZ_LH:> CompareTuple [TupleOne] [TupleTwo] <TupleConfigXmlFile>;");
			dbgui.addOutputPlainText("$SZ_LH:> LHClear <LHConfigXmlFile> <TupleConfigXmlFile>;");

		} else if (commands[0].equalsIgnoreCase("LHCreateEmpty")) {

			dbgui.addOutputPlainText(
					"$SZ_LH:> LHCreateEmpty <LHConfigXmlFile> <TupleConfigXmlFile><M><lambdaUpper><lambdaLower>;");//<totalPages>

			if (commands.length != 6) {
				dbgui.addOutputPlainText("Wrong command parameters, type 'list commands' for reference");
			} else {

				String lhConfigFile = dbgui.getVariableValue(commands[1].trim().substring(2));
				String tupleConfigFile = dbgui.getVariableValue(commands[2].trim().substring(2));
				Tuple tupleDefinition = new Tuple(tupleConfigFile);
				int M = Integer.parseInt(dbgui.getVariableValue(commands[3].trim().substring(2)));
				double lambdaUpper = Double.parseDouble(dbgui.getVariableValue(commands[4].trim().substring(2)));
				double lambdaLower = Double.parseDouble(dbgui.getVariableValue(commands[5].trim().substring(2)));
			//	int totalPages = Integer.parseInt(dbgui.getVariableValue(commands[6].trim().substring(2)));

				dbgui.addOutputPlainText(
						"Successfully parsesd arguments M is: " + String.valueOf(M) + ",lambdaUpper is: "
								+ String.valueOf(lambdaUpper) + ", lambdaLower is: " + String.valueOf(lambdaLower));

				LH lhashing = new LH(this.stoUtils, tupleDefinition);
				dbgui.addOutputPlainText("Create LH done!");
				lhashing.initializeLHWithoutPageIdsInLHConfig(lhConfigFile, M, lambdaUpper, lambdaLower);//,totalPages

				dbgui.addOutputPlainText("Successfully initialized a new LH");

				lhashing.updateLHConfigAndStorageCatalog(lhConfigFile);
			}

		} else if (commands[0].equalsIgnoreCase("LHInsert")) {

			dbgui.addOutputPlainText("$CyUtils:> LHInsert <LHConfigXmlFile> <TupleConfigXmlFile> [OneTuple];");

			if (commands.length != 4) {
				dbgui.addOutputPlainText("Wrong command parameters, type 'list commands' for reference");
			} else {

				String lhConfigFile = dbgui.getVariableValue(commands[1].trim().substring(2));
				String tupleConfigFile = dbgui.getVariableValue(commands[2].trim().substring(2));
				String tupleData = commands[3];
				dbgui.addOutputPlainText("tupleData is: " + tupleData);

				Tuple tupleDefinition = new Tuple(tupleConfigFile);
				LH lhashing = new LH(this.stoUtils, tupleDefinition);
				lhashing.initializeLHWithPageIdsInLHConfig(lhConfigFile);
				dbgui.addOutputPlainText("Successfully initialized a new LH with hashfile!");

				String currentContent = tupleData.substring(1, tupleData.length() - 1);
				dbgui.addOutputPlainText("currentContent of tuple data is: " + currentContent);

				String[] contentArray = currentContent.split(",");

				if (tupleDefinition.getAttributeNum() != contentArray.length) {
					dbgui.addOutputPlainText("Tuple format error!");

				} else {
					// currentLine -> buffer
					byte[] tuple = new byte[tupleDefinition.getLength()];
					int offset = 0;
					int count = 0;
					for (TupleAttribute tupleAttribute : tupleDefinition.getTupleAttributes()) {
						String attrType = tupleAttribute.getType();
						int attrLen = tupleAttribute.getLength();
						if (attrType.equals("Integer") && attrLen == 4) {
							// if it is an integer
							ReWriUtils.intToByteArray(Integer.parseInt(contentArray[count].trim()), tuple, offset);
						} else if (attrType.equals("String")) {
							// if it is a string
							ReWriUtils.stringToByteArray(contentArray[count], tuple, offset, offset + attrLen);
						}
						count++;
						offset += attrLen;
					}
					dbgui.addOutputPlainText("processed to-insert tuple is: " + Arrays.toString(tuple));

					try {
						lhashing.LHashingInsert(tuple);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						dbgui.addOutputPlainText("LHashingInsert error! ");
						e.printStackTrace();
					}
					dbgui.addOutputPlainText("Successfully insert this tuple!");
				}

				lhashing.updateLHConfigAndStorageCatalog(lhConfigFile);
				dbgui.addOutputPlainText("Successfully updated the lhConfigFile!");
			}

		} else if (commands[0].equalsIgnoreCase("CompareTuple")) {
			dbgui.addOutputPlainText("$SZ_LH:> CompareTuple [TupleOne] [TupleTwo] <TupleConfigXmlFile>");

			if (commands.length != 4) {
				dbgui.addOutputPlainText("Lack of command parameters, type 'list commands' for reference");
			} else {

				String tupleData1 = commands[1];
				String tupleData2 = commands[2];
				String tupleConfigFile = dbgui.getVariableValue(commands[3].trim().substring(2));

				String currentContent1 = tupleData1.substring(1, tupleData1.length() - 1);
				String[] contentArray1 = currentContent1.split(",");
				String currentContent2 = tupleData2.substring(1, tupleData2.length() - 1);
				String[] contentArray2 = currentContent2.split(",");
				Tuple tupleDefinition = new Tuple(tupleConfigFile);
				dbgui.addOutputPlainText("tuple attribute length is: " + tupleDefinition.getAttributeNum());

				if (tupleDefinition.getAttributeNum() != contentArray1.length
						|| tupleDefinition.getAttributeNum() != contentArray2.length) {
					dbgui.addOutputPlainText("Tuple format error!");

				} else {
					// currentLine -> buffer
					byte[] tuple1 = new byte[tupleDefinition.getLength()];
					byte[] tuple2 = new byte[tupleDefinition.getLength()];
					int offset = 0;
					int count = 0;
					for (TupleAttribute tupleAttribute : tupleDefinition.getTupleAttributes()) {
						String attrType = tupleAttribute.getType();
						int attrLen = tupleAttribute.getLength();
						if (attrType.equals("Integer") && attrLen == 4) {
							// if it is an integer
							ReWriUtils.intToByteArray(Integer.parseInt(contentArray1[count].trim()), tuple1, offset);
							ReWriUtils.intToByteArray(Integer.parseInt(contentArray2[count].trim()), tuple2, offset);
						} else if (attrType.equals("String")) {
							// if it is a string
							ReWriUtils.stringToByteArray(contentArray1[count], tuple1, offset, offset + attrLen);
							ReWriUtils.stringToByteArray(contentArray2[count], tuple2, offset, offset + attrLen);
						}
						count++;
						offset += attrLen;
					}
					dbgui.addOutputPlainText(
							"If given bytes error, we return 0. Otherwise -1: tuple1 < tuple2, 0: tuple1 == tuple2, 1: tuple1 > tuple2");
					int res = tupleDefinition.compare(tuple1, tuple2);
					dbgui.addOutputPlainText("Comparison result for tuple1 and tuple2 is: " + String.valueOf(res));
				}
			}

		} else if (commands[0].equalsIgnoreCase("LHClear")) {

			dbgui.addOutputPlainText("$SZ_LH:> LHClear <LHConfigXmlFile> <TupleConfigXmlFile>;");

			if (commands.length != 3) {
				dbgui.addOutputPlainText("Wrong command parameters, type 'list commands' for reference");
			} else {

				String lhConfigFile = dbgui.getVariableValue(commands[1].trim().substring(2));
				String tupleConfigFile = dbgui.getVariableValue(commands[2].trim().substring(2));
				Tuple tupleDefinition = new Tuple(tupleConfigFile);
				LH lhashing = new LH(this.stoUtils, tupleDefinition);
				dbgui.addOutputPlainText("Create LH done!");
				lhashing.initializeLHWithPageIdsInLHConfig(lhConfigFile);

				dbgui.addOutputPlainText("Successfully initialized a new LH from lhConfigFile!");

				lhashing.clearLHConfigAndStorageCatalog(lhConfigFile);
				dbgui.addOutputPlainText("Successfully cleared config and storage catalogue from lhConfigFile!");

			}
		} else {
			dbgui.addConsoleMessage("Wrong use of commands, type 'list commands' for reference");
		}

	} // end for execute method

}
