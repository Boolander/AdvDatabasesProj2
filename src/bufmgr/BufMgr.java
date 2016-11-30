package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 * 
 * @author Stefan Gurgurich
 * @author Christopher Booye
 */
public class BufMgr implements GlobalConst {

  //Array of frames of data
  Page[] buffer_pool;
  
  //Array of frame descriptions
  FrameDesc[] frametab;
  
  HashMap<Integer, Integer> page_to_frame;
  HashMap<Integer, Integer> frame_to_page;
  
  Replacer replacer;

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {
  
	//initialization of buffer_pool array. This will store each 'frame'
	buffer_pool = new Page[numframes];
	frametab = new FrameDesc[numframes];
	  
	//populates the buffer_pool and frametab arrays
	for (int i=0; i<frametab.length; i++){
 	buffer_pool[i] = new Page();
 	frametab[i] = new FrameDesc();
 	}
	  
	//creates an instance of replacer and initializes hashMaps
	replacer = new ReplacerImpl(this);
	page_to_frame = new HashMap<>();
    frame_to_page = new HashMap<>();
    
  } // public BufMgr(int numframes)

  /**
   * pinPage
   * 
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	//Search the hash map to see if a page number already exists for the frame
	Integer frame_num = page_to_frame.get(pageno.pid);
	
	if (frame_num == null){
		
		//There is no pre-existing frame number, so find a new one with 
		//the replacerimpl pickVictim method.
		frame_num = replacer.pickVictim();
			
		//A valid frame was found
		if (frame_num != -1){
      
			//If a frame was found to be valid, but had data in it, save the data to the disk
			//before overwriting it with new data and set the dirty flag to false.
			if ((frametab[frame_num].valid ==  true) && (frametab[frame_num].dirty == true)){
                Minibase.DiskManager.write_page(frametab[frame_num].pageno, buffer_pool[frame_num]);
				frametab[frame_num].dirty = false;
			}
        
			//Page data coming from disk.  Read and copy the data into the frame, 
			//set the mempage to it, update the frame descriptions and hashmap.
			if (contents == PIN_DISKIO){
				Page disk_page = new Page();
				Minibase.DiskManager.read_page(pageno, disk_page);
				buffer_pool[frame_num].copyPage(disk_page);
				mempage.setPage(buffer_pool[frame_num]);
				frametab[frame_num].pin_count++;  
				frametab[frame_num].valid = true; 
				frametab[frame_num].dirty = false; 
				frametab[frame_num].refbit = false; 
				frametab[frame_num].pageno = new PageId(pageno.pid); 
				updateHashMaps(pageno.pid, frame_num);       
			}
			//Page data coming from a mempage.  Copy the data into the frame, 
			//set the mempage to it, update the frame descriptions and hashmap.
			else if (contents == PIN_MEMCPY){
				buffer_pool[frame_num].copyPage(mempage);  
				mempage.setPage(buffer_pool[frame_num]);
				frametab[frame_num].pin_count++;  
				frametab[frame_num].valid = true;  
				frametab[frame_num].dirty = false; 
				frametab[frame_num].refbit = false; 
				frametab[frame_num].pageno = new PageId(pageno.pid); 
				updateHashMaps(pageno.pid, frame_num);    
			}
			else if (contents == PIN_NOOP){
				// No operation needed  
			}
			else{
				// Invalid operation, so error out
				throw new IllegalArgumentException();
			}
		}
		//No valid frame could be found, error out.
		else{
			throw new IllegalStateException(); 
		}
	}
    else{
      //Frame number already set for this page, so update the pin_count and
      //the mempage reference.
      frametab[frame_num].pin_count++;  
      mempage.setPage(buffer_pool[frame_num]);
    }
  } //pinPage
  
  /**
   * updateHashMaps
   * 
   * First, check to see if this page was already in the hashmaps; if it was remove the
   * references in both hash tables.  Then add the new data to both tables. 
   * 
   * @param page
   * @param frame
   */
  private void updateHashMaps(int page, int frame)
  {
    Integer oldPage = frame_to_page.get(frame);
    if (oldPage != null){
      page_to_frame.remove(oldPage);
      frame_to_page.remove(frame);
    }
    
    // Update both hashmaps
    page_to_frame.put(page, frame);
    frame_to_page.put(frame, page);			
		
  } //updateHashMaps
 
  /**
   * unpinPage
   * 
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

    Integer frame_num = page_to_frame.get(pageno.pid);
    if ((frame_num == null) || frametab[frame_num].pin_count == 0){
      //Trying to unpin a page that doesn't exist, so error out
      throw new IllegalArgumentException();      
    }
    else{
      if (dirty == true){
        //make sure the page stays dirty until saved to disk
        frametab[frame_num].dirty = dirty;
      }
      
      // Update the pin count.
      frametab[frame_num].pin_count--;
      if (frametab[frame_num].pin_count == 0){
        // Set reference bit to true if pin_count is 0.
        frametab[frame_num].refbit = true;
      }
    }
  } //unpinPage
  
  /**
   * newPage
   * 
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page first_page, int run_size) {

    if (getNumUnpinned() == 0){
      //Everything is already unpinned and the pool is clear, so error out.
    	throw new IllegalStateException();      
    }
    else{
      PageId pageno = Minibase.DiskManager.allocate_page(run_size);
  	  Integer frame_num = page_to_frame.get(pageno.pid);
    
      // The first page is already mapped into the buffer pool and pinned
  	  if ((frame_num != null) && (frametab[frame_num].pin_count > 0)){
        throw new IllegalArgumentException(); 
      }
      else{
        // Pin the first page and return its page id
        pinPage(pageno, first_page, PIN_MEMCPY);
        return pageno;
      }
    }
  } //newPage

  /**
   * freePage
   * 
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {

    Integer frame_num = page_to_frame.get(pageno.pid);
    
    //Frame number is assigned already and pinned, so error out
    if (frame_num != null && frametab[frame_num].pin_count >0){
    	throw new IllegalArgumentException();
    }
    else{
    	Minibase.DiskManager.deallocate_page(pageno);
    }
    
  } //freePage

  /**
   * flushAllPages
   * 
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllPages() {

    for (int i = 0; i < frametab.length; i++){
       	if ((frametab[i].dirty == true) && (frametab[i].valid == true)){
       		flushPage(frametab[i].pageno);
    		frametab[i].dirty = false;
    	}
    }

  } //flushAllFrames

  /**
   * flushPage
   * 
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {
	Integer frame_num = page_to_frame.get(pageno.pid);

	if(frame_num != null){
	  Minibase.DiskManager.write_page(pageno, buffer_pool[frame_num]);
	}
	else{
	  throw new IllegalArgumentException();  
	}    
  } //flushPage

  /**
   * getNumBuffers
   * 
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() {
    return frametab.length;
  } //getNumBuffers

  /**
   * getNumUnpinned
   * 
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    int total_unpinned = 0;
    
    for (int i = 0; i < frametab.length; i++){
      if (frametab[i].pin_count == 0){
      	total_unpinned++;
      }
    } 
    return total_unpinned;
  } //getNumUnpinned

} //BufMgr
