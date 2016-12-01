package heap;

import global.GlobalConst;
import global.RID;
import global.Minibase;
import global.PageId;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is an unordered set of records, stored on a set of pages. This
 * class provides basic support for inserting, selecting, updating, and deleting
 * records. Temporary heap files are used for external sorting and in other
 * relational operators. A sequential scan of a heap file (via the Scan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  static final short DATA_PAGE = 11;
  static final short DIR_PAGE = 12;
  PageId headId;
  Boolean tempFile;
  String fileName;
  
  /**
   * If the given name already denotes a file, this opens it; otherwise, this
   * creates a new empty file. A null name produces a temporary heap file which
   * requires no DB entry.
   */
  public HeapFile(String name) {
    if(name == null) {
		tempFile = true;
		fileName = "";
		headId = null;
	}
	else {
		tempFile = false;
		fileName = name;
		headId = Minibase.DiskManager.get_file_entry(fileName);
	}
	
	if(headId == null){
		DirPage dirPage = new DirPage();
		headId = Minibase.BufferManager.newPage(dirPage, 1);
		dirPage.setCurPage(headId);
		Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
		if(!tempFile){
			Minibase.DiskManager.add_file_entry(fileName, headId);
		}
			
	}
		
  }

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {
	if(tempFile)
	{
		deleteFile();
	}
  }

  /**
   * Deletes the heap file from the database, freeing all of its pages.
   */
  public void deleteFile() {
	PageId dirId = new PageId(headId.pid);
	DirPage dirPage = new DirPage();
	
	
	while(dirId.pid != INVALID_PAGEID){
		
		PageId currentPageId = new PageId(dirId.pid);
		Minibase.BufferManager.pinPage(currentPageId, dirPage, PIN_DISKIO);
		
		for(int i=0; i < dirPage.getEntryCnt(); i++){
			PageId dataId = dirPage.getPageId(i);
			Minibase.BufferManager.freePage(dataId);
		}
		
		Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
		Minibase.BufferManager.freePage(currentPageId);
		
		dirId = dirPage.getNextPage();
	}
	
	if(!tempFile){
		Minibase.DiskManager.delete_file_entry(fileName);
	}
			
  }

  /**
   * Inserts a new record into the file and returns its RID.
   * 
   * @throws IllegalArgumentException if the record is too large
   */
  public RID insertRecord(byte[] record) throws IllegalArgumentException{
	
	if(record.length > (PAGE_SIZE - DataPage.HEADER_SIZE - DataPage.SLOT_SIZE)){
		throw new IllegalArgumentException();
	}
	
	// algorithm starts with the head directory
	PageId dirId = new PageId(headId.pid);
	DirPage dirPage = new DirPage();
	PageId currentPageId = new PageId(dirId.pid);
	RID rid = null;
		
	while(dirId.pid != INVALID_PAGEID){
		
		currentPageId = new PageId(dirId.pid);
		Minibase.BufferManager.pinPage(currentPageId, dirPage, PIN_DISKIO);
		dirId = dirPage.getNextPage();
		
		for(int i=0; i < dirPage.getEntryCnt(); i++){
			
			if(dirPage.getFreeCnt(i) >= (record.length + DataPage.SLOT_SIZE)){
				
				// Found space for the record to be inserted
				PageId dataId = dirPage.getPageId(i);
				DataPage dataPage = new DataPage();
				Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
				rid = dataPage.insertRecord(record);
				
				dirPage.setRecCnt(i, dataPage.getSlotCount());
				dirPage.setFreeCnt(i, dataPage.getFreeSpace());
				Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
				
				break;
			}
		}
		
		if(rid != null){
			Minibase.BufferManager.unpinPage(currentPageId, UNPIN_DIRTY);
			break;
		}
		
		Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
	}
	
	if(rid == null){
		
		DataPage dataPage = new DataPage();
		PageId dataId = Minibase.BufferManager.newPage(dataPage, 1);
		dataPage.setCurPage(dataId);
		rid = dataPage.insertRecord(record);
		short slotCount = dataPage.getSlotCount();
		short freeSpace = dataPage.getFreeSpace();
		
		Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
		
		
		
		dirId = new PageId(headId.pid);
		boolean successAdd = false;
		
		while(dirId.pid != INVALID_PAGEID){
			
			currentPageId = new PageId(dirId.pid);
			Minibase.BufferManager.pinPage(currentPageId, dirPage, PIN_DISKIO);
			dirId = dirPage.getNextPage();
			
			short entryCount = dirPage.getEntryCnt();
			if(entryCount < DirPage.MAX_ENTRIES){
				
				dirPage.setPageId(entryCount, dataId);
				dirPage.setRecCnt(entryCount, slotCount);
				dirPage.setFreeCnt(entryCount, freeSpace);
				dirPage.setEntryCnt(entryCount++);
				Minibase.BufferManager.unpinPage(currentPageId, UNPIN_DIRTY);
				successAdd = true;
				break;
			}
			
			Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
		}
		
		if(!successAdd){
			
			Minibase.BufferManager.pinPage(currentPageId, dirPage, PIN_DISKIO);
			
			DirPage newDirPage = new DirPage();
			PageId newDirId = Minibase.BufferManager.newPage(newDirPage, 1);
			newDirPage.setCurPage(newDirId);
			newDirPage.setPageId(0, dataId);
			newDirPage.setRecCnt(0, slotCount);
			newDirPage.setFreeCnt(0, freeSpace);
			newDirPage.setEntryCnt((short)1);
			
			dirPage.setNextPage(newDirId);
			newDirPage.setPrevPage(currentPageId);
			
			Minibase.BufferManager.unpinPage(newDirId, UNPIN_DIRTY);
			Minibase.BufferManager.unpinPage(currentPageId, UNPIN_DIRTY);
		}
	}
	
	return rid;
  }

  /**
   * Reads a record from the file, given its id.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) throws IllegalArgumentException {
	
    byte[] record;
    DataPage dataPage = new DataPage();
    Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
    
    try
    {
      record = dataPage.selectRecord(rid);
    }
    catch (Exception e)
    {
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
      throw new IllegalArgumentException();            
    }
    
    Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
    return record;
	
  }

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {

	if (rid == null || newRecord == null)
    {
      throw new IllegalArgumentException();
    }

    DataPage page = new DataPage();
    Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
    try
    {
      page.updateRecord(rid, newRecord);
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
    }
    catch(IllegalArgumentException exception)
    {
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
      throw exception;
    }
  }

  /**
   * Deletes the specified record from the heap file.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) throws IllegalArgumentException {

	if(rid == null) {
		throw new IllegalArgumentException();
	}
	
	DataPage dataPage = new DataPage();
	Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);

	short recordLength = dataPage.getSlotLength(rid.slotno);
	
	try
	{
		dataPage.deleteRecord(rid);
		Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
	}
	catch(IllegalArgumentException exception)
	{
		Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
		throw exception;
	}
	
	DirPage dirPage = new DirPage();
	PageId dirId = new PageId(headId.pid);
	
	while(dirId.pid != INVALID_PAGEID){
		PageId curPageId = new PageId(dirId.pid);
		Minibase.BufferManager.pinPage(curPageId, dirPage, PIN_DISKIO);
		dirId = dirPage.getNextPage();

		for (short i=0; i < dirPage.getEntryCnt(); i++){
			if (dirPage.getPageId(i).pid == rid.pageno.pid){
				short newRecCnt = dirPage.getRecCnt(i);
				newRecCnt--;
				dirPage.setRecCnt(i, newRecCnt);
				  
				short newFreeCnt = dirPage.getFreeCnt(i);
				newFreeCnt += recordLength;
				dirPage.setFreeCnt(i, newFreeCnt);
				  
				if (newRecCnt < 1){
					dirPage.compact(i);
					  
					Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);  
					Minibase.BufferManager.freePage(rid.pageno);
					short newEntryCnt = dirPage.getEntryCnt();
					  
					if (newEntryCnt < 1) {
						  if (curPageId.pid == headId.pid) {
							Minibase.BufferManager.unpinPage(curPageId, UNPIN_DIRTY);
							break;
						  }
						  else {
							DirPage parentDirPage = new DirPage();
							Minibase.BufferManager.pinPage(dirPage.getPrevPage(), parentDirPage, PIN_DISKIO);
							
							parentDirPage.setNextPage(dirPage.getNextPage());
							  
							if(dirPage.getNextPage().pid != INVALID_PAGEID) {
								DirPage childDirPage = new DirPage();
								Minibase.BufferManager.pinPage(dirPage.getNextPage(), childDirPage, PIN_DISKIO);
								
								childDirPage.setPrevPage(dirPage.getPrevPage());
								Minibase.BufferManager.unpinPage(dirPage.getNextPage(), UNPIN_DIRTY);
							}

							Minibase.BufferManager.unpinPage(dirPage.getPrevPage(), UNPIN_DIRTY);  
						}
						  
						Minibase.BufferManager.unpinPage(curPageId, UNPIN_DIRTY);
						Minibase.BufferManager.freePage(curPageId);
						break;
					}
				}
			}	  
		}
		
		Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);	
	}
  }

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {
      
	int count = 0;
    DirPage dirPage = new DirPage();
    PageId dirId = new PageId(headId.pid);
	
	while(dirId.pid != INVALID_PAGEID)
    {
      PageId curPageId = new PageId(dirId.pid);
      Minibase.BufferManager.pinPage(curPageId, dirPage, PIN_DISKIO);
      dirId = dirPage.getNextPage();

      for (short i=0; i < dirPage.getEntryCnt(); i++)
      {
        count = count + dirPage.getRecCnt(i);
      }
      
      Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
    }

    return count;
  }

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
	return fileName;
  }

} // public class HeapFile implements GlobalConst
