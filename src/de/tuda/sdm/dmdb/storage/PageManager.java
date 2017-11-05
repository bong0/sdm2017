package de.tuda.sdm.dmdb.storage;

import de.tuda.sdm.dmdb.storage.exercise.RowPage;

/**
 * Page managetr to creates new pages
 * @author cbinnig
 *
 */
public class PageManager {
	/**
	 * Creates a page for a given page type and slot size
	 * @param type Enum PageType
	 * @param slotSize Size of lot in bytes
	 * @return
	 */
	public static AbstractPage createPage(EnumPageType type, int slotSize){
		switch(type){
		case RowPageType:
			return new RowPage(slotSize);
		}
		return new RowPage(slotSize);
	}
	
	/**
	 * Creates a page with a default pyge type and a given slot size
	 * @param slotSize
	 * @return
	 */
	public static AbstractPage createDefaultPage(int slotSize){
		return new RowPage(slotSize);
	}
}
